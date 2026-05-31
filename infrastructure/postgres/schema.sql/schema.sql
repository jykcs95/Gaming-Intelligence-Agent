CREATE EXTENSION IF NOT EXISTS vector;

---------------------------------------------------
-- Steam Event Storage
---------------------------------------------------

CREATE TABLE steam_events
(
    id BIGSERIAL PRIMARY KEY,

    gid VARCHAR(128) NOT NULL UNIQUE,

    app_id INTEGER NOT NULL,

    title TEXT,

    event_type VARCHAR(100),

    event_time TIMESTAMP,

    raw_payload JSONB,

    created_at TIMESTAMP DEFAULT NOW()
);

---------------------------------------------------
-- Deduplication Ledger
---------------------------------------------------

CREATE TABLE processed_message_ledger
(
    gid VARCHAR(128) PRIMARY KEY,

    processed_at TIMESTAMP DEFAULT NOW()
);

---------------------------------------------------
-- AI Analysis
---------------------------------------------------

CREATE TABLE ai_analysis
(
    id BIGSERIAL PRIMARY KEY,

    gid VARCHAR(128) NOT NULL,

    summary TEXT,

    sentiment VARCHAR(50),

    confidence DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT NOW()
);

---------------------------------------------------
-- Vector Embeddings
---------------------------------------------------

CREATE TABLE embeddings
(
    id BIGSERIAL PRIMARY KEY,

    gid VARCHAR(128) NOT NULL,

    content TEXT NOT NULL,

    embedding VECTOR(768),

    created_at TIMESTAMP DEFAULT NOW()
);

---------------------------------------------------
-- Vector Similarity Search
---------------------------------------------------

CREATE INDEX idx_embeddings_cosine
ON embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

---------------------------------------------------
-- Query Performance
---------------------------------------------------

CREATE INDEX idx_events_gid
ON steam_events(gid);

CREATE INDEX idx_events_time
ON steam_events(event_time);

CREATE INDEX idx_ai_gid
ON ai_analysis(gid);

CREATE INDEX IF NOT EXISTS idx_embeddings_vector
ON embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
