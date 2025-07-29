CREATE TYPE crawl_status AS ENUM (
    'pending',
    'completed'
);

CREATE TABLE urls (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    crawl_status crawl_status NOT NULL DEFAULT 'pending',
    fetched_at TIMESTAMP
);
