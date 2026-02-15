CREATE TABLE urls (
    url TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
	content_summary TEXT NOT NULL,
	page_rank DOUBLE PRECISION,
    crawled_at TIMESTAMP NOT NULL
);

CREATE TABLE robot_rules (
    domain TEXT PRIMARY KEY,
    rules_json JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE links (
	"from" TEXT NOT NULL,
	"to" TEXT NOT NULL,

    CONSTRAINT pk_links PRIMARY KEY ("from", "to"),
    CONSTRAINT fk_links FOREIGN KEY ("from") REFERENCES urls(url) ON DELETE CASCADE
);
CREATE INDEX idx_links_from ON links("from");
CREATE INDEX idx_links_to ON links("to");

CREATE TABLE word_data (
    word TEXT NOT NULL,
    url TEXT NOT NULL,
    position_bits BYTEA NOT NULL,
    term_frequency INTEGER NOT NULL,
    idf DOUBLE PRECISION,
    tf_idf DOUBLE PRECISION,

    CONSTRAINT pk_word_data PRIMARY KEY (word, url),
    CONSTRAINT fk_word_data_url_id FOREIGN KEY (url) REFERENCES urls(url) ON DELETE CASCADE
);
CREATE INDEX idx_word_data_word ON word_data(word);
CREATE INDEX idx_word_data_url_id ON word_data(url);
