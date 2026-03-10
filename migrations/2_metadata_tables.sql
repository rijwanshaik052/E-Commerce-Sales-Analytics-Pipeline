CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT

);

CREATE TABLE IF NOT EXISTS metadata.file_ingestion_logs (
    id SERIAL PRIMARY KEY,
    file_name TEXT,
    load_time TIMESTAMP,
    records_loaded INT,
    records_failed INT,
    status TEXT
);