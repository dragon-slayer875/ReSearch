CREATE TYPE crawl_status AS ENUM (
    'pending',
    'completed'
);

CREATE TABLE urls (
    url TEXT PRIMARY KEY,
    fetched_at TIMESTAMP
);

CREATE TABLE robot_rules (
    domain TEXT PRIMARY KEY,
    rules JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL
);
