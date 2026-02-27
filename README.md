# reSearch --- A search engine, from scratch

reSearch is a minimal yet functional implementation of a search engine system. It is primarily designed for educational purposes and personal use, but feel free to give it a try.

## Features

- **Robots.txt parsing**: The crawler crawls the web while respecting robots.txt rules. 
- **Web searching**: Users can search the web using keywords. The search results are ranked using Tf-Idf scores and Google's original Page Rank algorithm. 
- **Crawlerboard**: A public crawlerboard that allows users to add urls/pages they wish to be crawled and indexed. 
- **Admin controls**: Admin control over the crawlerboard to accept and reject resource crawling requests.

## Architecture

reSearch takes advantage of microservices architecture, allowing it to distribute its work among various services that can be deployed independently. Each component of reSearch is developed as a self contained service, which allows for easy horizontal scaling and maintenance.

reSearch uses Redis as an in memory data store, distributed lock and job queue, and PostgreSQL as the primary database for storing indexed data.

The `services` directory houses all the services each with its own Dockerfile and docker compose for convenient deployment.

## Services

- **Crawler**: Responsible for crawling the web, the crawler uses domain specific queues to enforce polite crawling and to discover and queue links in a breadth first manner. The crawled data is stored in Redis along with an outlink map of each crawled page.
- **Indexer**: It processes the crawled pages from Redis and indexes them using an inverted index structure. It stores the data in a PostgreSQL database, which has relations between words and web pages and links between all the pages.
- **Tf-Idf**: Calculates the term frequency-inverse document frequency (tf-idf) for each term in the indexed pages and updates the database relations. Tf-Idf scoring allows for determination of importance for term in context of all the indexed pages. 
- **Page Rank**: Uses the stored outlinks and backlinks data to calculate page rank, and thus determine the importance of each page using Google's original page rank algorithm. The updated ranks are stored in the PostgreSQL database.
- **Server**: Processes user queries and provides search results from the indexed pages. Uses simple keywords based matching to find relevant pages, and a combination of tf-idf scores and page ranks to rank the results. Provides endpoints that serve both JSON and HTML data.

## Workflow

1. Crawlers crawl the web while respecting robots.txt for each domain, store the crawled content and push the url in a queue in Redis for indexing
2. Indexer processes urls from the queue and updates the search index (in PostgreSQL).
3. The Tf-Idf service runs at regular, configurable intervals and calculates tf-idf scores for each term in the indexed web pages.
4. Page Rank service fetches all discovered (not necessarily crawled) urls from the database, along with their backlinks and outlinks information and calculates their ranks at configured intervals as well.
5. The server provides API and web GUI interfaces to allow users to interact with the system. It handles query processing, returning results and the public crawlerboard.

## Usage

- Clone the repository.
- Install [Docker](https://www.docker.com/)  and [Docker Compose](https://docs.docker.com/compose/) for your platform.

- All services:
    - Create .env file in the services directory, containing same keys as the provided [.env.example](https://github.com/dragon-slayer875/reSearch/blob/77a060638c85c51e2dbfa618d96f92e46c31dc79/services/.env.example).
    - Update the config.yaml file as needed.
    - The [Docker Compose file](https://github.com/dragon-slayer875/reSearch/blob/77a060638c85c51e2dbfa618d96f92e46c31dc79/services/docker-compose.yaml) in the `services` directory can be used to deploy all the services in a single go.
    - Change into the `services` directory and do `docker compose up` to run the services.

- Individual services:
    - Each service can use values from the common [.env.example](https://github.com/dragon-slayer875/reSearch/blob/77a060638c85c51e2dbfa618d96f92e46c31dc79/services/.env.example). Even so, .env.example files are provided in each service's directory for convenience.
    - Only the [server](https://github.com/dragon-slayer875/reSearch/tree/77a060638c85c51e2dbfa618d96f92e46c31dc79/services/server) can automatically apply migrations and thus you will need to run migrations manually if you intend to run any other service first. 
    - Change into the service's directory and do `docker compose up` to run the service.

## Tech Stack

- **Redis** as in memory key value based data storage, job queue and distributed lock.
- **PostgreSQL** for storing indexed data.
- **Docker** for service containerization.
- **Fiber** as a web server and interface allowing users to interact with the search engine  
- **HTML, CSS and JS** for creating the web GUI.
