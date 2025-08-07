CREATE TABLE urls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url TEXT NOT NULL UNIQUE,
    fetched_at TIMESTAMP
);
CREATE INDEX idx_urls_url ON urls(url);

CREATE TABLE robot_rules (
    domain TEXT PRIMARY KEY,
    rules_json JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL
);

CREATE TABLE metadata (
    url_id UUID PRIMARY KEY,
    title TEXT,
    meta_title TEXT,
    meta_description TEXT,
    meta_robots TEXT,

    CONSTRAINT fk_metadata_url FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE CASCADE
);
CREATE INDEX idx_metadata_url_id ON metadata(url_id);

CREATE TABLE inverted_index (
    word TEXT PRIMARY KEY,
    document_bits BYTEA NOT NULL
);

CREATE TABLE word_data (
    word TEXT NOT NULL,
    url_id UUID NOT NULL,
    position_bits BYTEA NOT NULL,
    term_frequency INTEGER NOT NULL,

    CONSTRAINT pk_word_data PRIMARY KEY (word, url_id),
    CONSTRAINT fk_word_data_url_id FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE CASCADE,
    CONSTRAINT fk_word_data_word FOREIGN KEY (word) REFERENCES inverted_index(word) ON DELETE CASCADE
);
