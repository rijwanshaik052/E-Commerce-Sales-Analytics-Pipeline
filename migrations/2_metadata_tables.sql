CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT

);

CREATE TABLE IF NOT EXISTS metadata.file_ingestion_logs (
    id BIGSERIAL PRIMARY KEY,
    file_name TEXT,
    file_hash TEXT,
    file_size BIGINT,
    status TEXT,
    ingestion_time TIMESTAMP,
    row_count INT,
    error_message TEXT
);

ALTER TABLE metadata.file_ingestion_logs
ADD CONSTRAINT unique_file_hash UNIQUE (file_hash);

