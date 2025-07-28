CREATE TABLE urls (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    status_code INTEGER NOT NULL DEFAULT 0,
    fetched_at TIMESTAMP
);
