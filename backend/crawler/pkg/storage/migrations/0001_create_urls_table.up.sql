CREATE TABLE urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
	page_rank DOUBLE PRECISION,
    fetched_at TIMESTAMP
);
CREATE INDEX idx_urls_url ON urls(url);

CREATE TABLE robot_rules (
    domain TEXT PRIMARY KEY,
    rules_json JSONB NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE links (
	"from" BIGSERIAL NOT NULL,
	"to" BIGSERIAL NOT NULL,

    CONSTRAINT pk_links PRIMARY KEY ("from", "to"),
    CONSTRAINT fk_links_from FOREIGN KEY ("from") REFERENCES urls(id) ON DELETE CASCADE,
    CONSTRAINT fk_links_to FOREIGN KEY ("to") REFERENCES urls(id) ON DELETE CASCADE
);
CREATE INDEX idx_links_from ON links("from");
CREATE INDEX idx_links_to ON links("to");

CREATE TABLE url_data (
    url_id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
	raw_content TEXT NOT NULL,

    CONSTRAINT fk_url_data_id FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE CASCADE
);

CREATE TABLE word_data (
    word TEXT NOT NULL,
    url_id BIGSERIAL NOT NULL,
    position_bits BYTEA NOT NULL,
    term_frequency INTEGER NOT NULL,
    idf DOUBLE PRECISION,
    tf_idf DOUBLE PRECISION,

    CONSTRAINT pk_word_data PRIMARY KEY (word, url_id),
    CONSTRAINT fk_word_data_url_id FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE CASCADE
);
CREATE INDEX idx_word_data_word ON word_data(word);
CREATE INDEX idx_word_data_url_id ON word_data(url_id);
