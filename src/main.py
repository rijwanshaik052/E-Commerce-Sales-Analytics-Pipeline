import logging
from ingest import process_files
from metadata_db import create_table, insert_file_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # Step 1: Create metadata table
    create_table()

    # Step 2: Define folders
    incoming_folder = "data/incoming"
    processed_folder = "data/processed"
    rejected_folder = "data/rejected"
    audit_file = "data/audit/audit_log.csv"

    # Step 3: Process files
    valid_files, invalid_files = process_files(
        incoming_folder,
        processed_folder,
        rejected_folder,
        audit_file,
    )

    # Step 4: Store metadata (SUCCESS)
    for file_name, destination in valid_files:
        try:
            insert_file_metadata(
                file_name=file_name,
                file_size=0,  # you can improve later
                file_type=file_name.split(".")[-1],
                file_path=destination,
                s3_key=f"bronze/{file_name}",
                row_count=0,
                status="SUCCESS"
            )
        except Exception as e:
            logger.error("Metadata insert failed for %s: %s", file_name, e)

    # Step 5: Store metadata (FAILED)
    for file_name, reason, destination in invalid_files:
        try:
            insert_file_metadata(
                file_name=file_name,
                file_size=0,
                file_type=file_name.split(".")[-1],
                file_path=destination,
                s3_key=None,
                row_count=0,
                status="FAILED",
                error_message=reason
            )
        except Exception as e:
            logger.error("Metadata insert failed for %s: %s", file_name, e)


if __name__ == "__main__":
    main()