package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"indexer/internals/queue"
	"indexer/internals/storage/database"
	"io"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Indexer struct {
	logger      *log.Logger
	workerCount int
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	ctx         context.Context
}

var (
	errNoJobs = errors.New("no jobs available")
)

func NewIndexer(logger *log.Logger, workerCount int, redisClient *redis.Client, dbPool *pgxpool.Pool, ctx context.Context) *Indexer {
	return &Indexer{
		logger,
		workerCount,
		redisClient,
		dbPool,
		ctx,
	}
}

func (i *Indexer) Start() {
	var wg sync.WaitGroup

	for range i.workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i.worker()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.updateConsolidator()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		// transfer urls in processing queue which are older than 30 minute to pending queue
		for {
			processingJobs, err := i.redisClient.ZRangeByScore(i.ctx, queue.ProcessingQueue, &redis.ZRangeBy{
				Min:    "0",
				Max:    strconv.FormatInt(time.Now().Add(-time.Minute*30).Unix(), 10),
				Offset: 0,
				Count:  100,
			}).Result()
			if err != nil {
				i.logger.Println("Error fetching processing jobs:", err)
				time.Sleep(2 * time.Second)
				continue
			}

			if len(processingJobs) == 0 {
				time.Sleep(2 * time.Second)
				continue
			}

			for _, job := range processingJobs {
				if err := i.requeueJob(job); err != nil {
					i.logger.Println("Error requeuing job:", job, "Error:", err)
				}
			}

		}

	}()
	wg.Wait()
}

func (i *Indexer) worker() {
	for {
		jobDetailsStr, err := i.getNextJob()
		if err != nil {
			i.logger.Println(err)
			continue
		}

		job := &queue.IndexJob{}
		if err := json.Unmarshal([]byte(jobDetailsStr), job); err != nil {
			i.logger.Printf("Error unmarshaling job: %v\n", err)
			continue
		}

		i.logger.Printf("Processing job: %d\n", job.JobId)

		processedJob, err := i.processJob(job)
		if err != nil && err != io.EOF {
			i.logger.Printf("Error processing job %s: %v\n", job.Url, err)
			continue
		}

		postingsList, err := i.createPostingsList(processedJob.cleanTextContent, job.JobId)
		if err != nil {
			i.logger.Printf("Error creating postings list for job %s: %v\n", job.Url, err)
			continue
		}

		if err := i.updateQueues(postingsList, job.Url, job.JobId, processedJob); err != nil {
			i.logger.Printf("Error updating queues for job %s: %v\n", job.Url, err)
			continue
		}

		i.logger.Println("Successfully processed job:", job.JobId)
	}
}

func (i *Indexer) getNextJob() (string, error) {
	result, err := i.redisClient.BRPop(i.ctx, time.Second*20, queue.PendingQueue).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", errNoJobs
		}
		return "", err
	}

	// 1 is job, 0 is queue name
	job := result[1]

	pipe := i.redisClient.Pipeline()
	pipe.ZAdd(i.ctx, queue.ProcessingQueue, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: job,
	})

	getCmd := pipe.Get(i.ctx, job)

	_, err = pipe.Exec(i.ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get next job: %w", err)
	}

	jobDetailsStr, err := getCmd.Result()
	if err != nil {
		return "", fmt.Errorf("failed to get job details: %w", err)
	}

	return jobDetailsStr, nil
}

func (i *Indexer) updateQueues(postingsList *map[string]*Posting, jobUrl string, jobId int64, processedJob *processedJob) error {
	pipe := i.redisClient.Pipeline()

	queries := database.New(i.dbPool)
	err := queries.InsertUrlData(i.ctx, database.InsertUrlDataParams{
		UrlID:       jobId,
		Title:       processedJob.title,
		Description: processedJob.description,
		RawContent:  processedJob.rawTextContent})

	if err != nil {
		return fmt.Errorf("failed to queue updates: %w", err)
	}

	for word, posting := range *postingsList {
		positionsBytes, err := posting.Positions.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to marshal positions for word %s: %w", word, err)
		}
		postingSerialized := &database.BatchInsertWordDataParams{
			Word:          word,
			UrlID:         posting.DocId,
			TermFrequency: posting.Tf,
			PositionBits:  positionsBytes,
		}
		postingJson, err := json.Marshal(postingSerialized)
		if err != nil {
			return fmt.Errorf("failed to marshal posting for word %s: %w", word, err)
		}
		pipe.LPush(i.ctx, queue.PostingPendingQueue, string(postingJson))
	}

	pipe.ZRem(i.ctx, queue.ProcessingQueue, jobUrl)

	_, err = pipe.Exec(i.ctx)
	if err != nil {
		return fmt.Errorf("failed to queue updates: %w", err)
	}

	return nil
}

func (i *Indexer) updateConsolidator() {
	pool := goredis.NewPool(i.redisClient)
	rs := redsync.New(pool)
	for {
		select {
		case <-time.After(3 * time.Minute):
			mutex := rs.NewMutex("lock:consolidator")
			if err := mutex.Lock(); err != nil {
				i.logger.Println("Error acquiring consolidator lock:", err)
				continue
			}

			i.logger.Println("Consolidator running...")

			postings, err := i.redisClient.RPopCount(i.ctx, queue.PostingPendingQueue, 5000).Result()
			if err != nil {
				i.logger.Println("Error getting pending postings from queue:", err)
				if ok, err := mutex.Unlock(); !ok || err != nil {
					i.logger.Println("Error releasing consolidator lock:", err)
				}
				continue
			}

			if err := i.redisClient.LPush(i.ctx, queue.PostingProcessingQueue, postings).Err(); err != nil {
				i.logger.Println("Error moving postings to processing queue:", err)
				if ok, err := mutex.Unlock(); !ok || err != nil {
					i.logger.Println("Error releasing consolidator lock:", err)
				}
				continue
			}

			invertedIndexBatch := make(map[string]InvertedIndex)
			postingsBatch := make([]database.BatchInsertWordDataParams, 0, len(postings))
			for _, postingStr := range postings {
				var posting database.BatchInsertWordDataParams
				if err := json.Unmarshal([]byte(postingStr), &posting); err != nil {
					i.logger.Println("Error deserializing posting from queue:", err)
				}
				postingsBatch = append(postingsBatch, posting)

				if existingIndex, exists := invertedIndexBatch[posting.Word]; exists {
					added := existingIndex.DocBitmap.CheckedAdd(uint64(posting.UrlID))
					if added {
						existingIndex.DocFreq++
					}
				} else {
					docBitmap := roaring64.New()
					docBitmap.Add(uint64(posting.UrlID))
					invertedIndexBatch[posting.Word] = InvertedIndex{
						Word:      posting.Word,
						DocBitmap: docBitmap,
						DocFreq:   1,
					}
				}
			}

			if err := i.updateStorage(&invertedIndexBatch, &postingsBatch); err != nil {
				i.logger.Println("Error updating storage:", err)
			}

			if err := i.redisClient.LTrim(i.ctx, queue.PostingProcessingQueue, int64(len(postings)), -1).Err(); err != nil {
				i.logger.Println("Error clearing processing queue:", err)
				continue
			}

			if ok, err := mutex.Unlock(); !ok || err != nil {
				i.logger.Println("Error releasing consolidator lock:", err)
				continue
			}

			i.logger.Printf("Consolidation complete")

		case <-i.ctx.Done():
			return
		}
	}
}

func (i *Indexer) updateStorage(invertedIndexMap *map[string]InvertedIndex, insertPostingsBatch *[]database.BatchInsertWordDataParams) error {
	tx, err := i.dbPool.Begin(i.ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(i.ctx)

	queries := database.New(i.dbPool)
	queriesWithTx := queries.WithTx(tx)

	getInvertedIndexBatch := make([]string, 0, len(*invertedIndexMap))
	insertInvertedIndexBatch := make([]database.BatchInsertInvertedIndexParams, 0, len(*invertedIndexMap))
	updateInvertedIndexBatch := make([]database.BatchUpdateInvertedIndexByWordParams, 0, len(*invertedIndexMap))
	invertedIndexResults := make(map[string]database.InvertedIndex)

	for word := range *invertedIndexMap {
		getInvertedIndexBatch = append(getInvertedIndexBatch, word)
	}

	var insertErrs, updateErrs, getErrs []error
	getQuery := queriesWithTx.BatchGetInvertedIndexByWord(i.ctx, getInvertedIndexBatch)

	storedInvertedIndices := make(map[string]database.InvertedIndex)
	getQuery.QueryRow(func(i int, ii database.InvertedIndex, err error) {
		if err != nil && err != pgx.ErrNoRows {
			getErrs = append(getErrs, err)
			return
		}
		if err == pgx.ErrNoRows {
			return
		}
		storedInvertedIndices[ii.Word] = ii
	})

	if len(getErrs) > 0 {
		return fmt.Errorf("failed to execute batch get for inverted index: %v", getErrs)
	}

	for word, storedInvertedIndex := range storedInvertedIndices {
		existingDocumentBitmap := roaring64.New()
		_, err = existingDocumentBitmap.FromUnsafeBytes(storedInvertedIndex.DocumentBits)
		if err != nil {
			return fmt.Errorf("failed to unmarshal existing bitmap for word %s: %w", word, err)
		}
		(*invertedIndexMap)[word].DocBitmap.Or(existingDocumentBitmap)
		documentBitmapBytes, err := (*invertedIndexMap)[word].DocBitmap.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to marshal updated bitmap for word %s: %w", word, err)
		}
		updateInvertedIndexBatch = append(updateInvertedIndexBatch, database.BatchUpdateInvertedIndexByWordParams{
			Word:         word,
			DocumentBits: documentBitmapBytes,
			DocFrequency: storedInvertedIndex.DocFrequency + (*invertedIndexMap)[word].DocFreq,
		})
	}
	for word, invertedIndex := range *invertedIndexMap {
		if _, exists := storedInvertedIndices[word]; !exists {
			documentBitmapBytes, err := invertedIndex.DocBitmap.ToBytes()
			if err != nil {
				return fmt.Errorf("failed to marshal bitmap for word %s: %w", word, err)
			}
			insertInvertedIndexBatch = append(insertInvertedIndexBatch, database.BatchInsertInvertedIndexParams{
				Word:         word,
				DocumentBits: documentBitmapBytes,
				DocFrequency: invertedIndex.DocFreq,
			})
		}
	}

	insertQuery := queriesWithTx.BatchInsertInvertedIndex(i.ctx, insertInvertedIndexBatch)
	insertQuery.QueryRow(func(i int, ii database.InvertedIndex, err error) {
		if err != nil {
			insertErrs = append(insertErrs, err)
			return
		}
		invertedIndexResults[ii.Word] = ii
	})

	updateQuery := queriesWithTx.BatchUpdateInvertedIndexByWord(i.ctx, updateInvertedIndexBatch)
	updateQuery.QueryRow(func(i int, ii database.InvertedIndex, err error) {
		if err != nil {
			updateErrs = append(updateErrs, err)
			return
		}
		invertedIndexResults[ii.Word] = ii
	})

	if len(insertErrs) > 0 {
		return fmt.Errorf("failed to execute batch insert for inverted index: %v", insertErrs)
	}
	if len(updateErrs) > 0 {
		return fmt.Errorf("failed to execute batch update for inverted index: %v", updateErrs)
	}

	_, err = queriesWithTx.BatchInsertWordData(i.ctx, *insertPostingsBatch)
	if err != nil {
		return fmt.Errorf("failed to batch insert word data: %w", err)
	}

	totalIndexedDocs, err := queriesWithTx.GetTotalIndexedDocumentCount(i.ctx)
	if err != nil {
		return fmt.Errorf("failed to get total indexed document count: %w", err)
	}

	updateIdfBatch := make([]database.UpdateWordIdfParams, 0, len(invertedIndexResults))
	for _, invertedIndex := range invertedIndexResults {
		idf := 0.0
		idf = float64(totalIndexedDocs) / float64(invertedIndex.DocFrequency)
		idf = 1 + math.Log10(idf)
		updateIdfBatch = append(updateIdfBatch, database.UpdateWordIdfParams{
			Word: invertedIndex.Word,
			Idf:  pgtype.Float8{Float64: idf, Valid: true},
		})
	}

	var updateTfIdfErrs []error
	updateTfIdfBatch := make([]database.UpdateWordTfIdfParams, 0, 5000)
	updateWordIdfQuery := queriesWithTx.UpdateWordIdf(i.ctx, updateIdfBatch)
	updateWordIdfQuery.Query(func(success int, uwir []database.UpdateWordIdfRow, err error) {
		if err != nil {
			updateTfIdfErrs = append(updateTfIdfErrs, err)
			return
		}
		for _, row := range uwir {
			updateTfIdfBatch = append(updateTfIdfBatch, database.UpdateWordTfIdfParams{
				Word:  row.Word,
				UrlID: row.UrlID,
				TfIdf: pgtype.Float8{Float64: float64(row.TermFrequency) * row.Idf.Float64, Valid: true},
			})
		}
	})

	var updateTfIdfBatchErrs []error
	queriesWithTx.UpdateWordTfIdf(i.ctx, updateTfIdfBatch).QueryRow(func(count int, uwtir database.UpdateWordTfIdfRow, err error) {
		if err != nil {
			updateTfIdfBatchErrs = append(updateTfIdfBatchErrs, err)
			return
		}

		i.logger.Printf("Updated TF-IDF for word %s in document %d\n", uwtir.Word, uwtir.UrlID)
	})

	if len(updateTfIdfErrs) > 0 {
		return fmt.Errorf("failed to execute batch update for word idf: %v", updateTfIdfErrs)
	}

	return tx.Commit(i.ctx)
}

func (i *Indexer) requeueJob(job string) error {
	pipe := i.redisClient.Pipeline()

	pipe.LPush(i.ctx, queue.PendingQueue, job)
	pipe.ZRem(i.ctx, queue.ProcessingQueue, job)

	_, err := pipe.Exec(i.ctx)
	if err != nil {
		return fmt.Errorf("failed to requeue job %s: %w", job, err)
	}

	return nil
}
