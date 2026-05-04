from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="olist_ingestion_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "s3", "metadata"],
)
def olist_ingestion_pipeline():

    @task
    def run_ingestion():
        from src.ingest import process_files

        process_files(
            incoming_folder="data/incoming",
            processed_folder="data/processed",
            bucket_name="shaik-olist-bucket"
        )

    run_ingestion()

olist_ingestion_pipeline()