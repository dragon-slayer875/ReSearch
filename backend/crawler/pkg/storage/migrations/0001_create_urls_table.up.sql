CREATE TYPE crawl_status AS ENUM (
    'pending',
    'completed'
);

CREATE TABLE urls (
    url TEXT PRIMARY KEY,
    crawl_status crawl_status NOT NULL DEFAULT 'pending',
    fetched_at TIMESTAMP
);
