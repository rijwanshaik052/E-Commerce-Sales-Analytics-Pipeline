
from pathlib import Path
from datetime import datetime
import logging
import shutil
import csv
import pandas as pd
from upload_files_to_s3 import upload_to_s3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


EXPECTED_CUSTOMERS_COLUMNS = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
]

EXPECTED_ORDERS_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

EXPECTED_ORDER_ITEMS_COLUMNS = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

EXPECTED_PAYMENTS_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]

SCHEMA_MAP = {
    "customers": EXPECTED_CUSTOMERS_COLUMNS,
    "orders": EXPECTED_ORDERS_COLUMNS,
    "order_items": EXPECTED_ORDER_ITEMS_COLUMNS,
    "payments": EXPECTED_PAYMENTS_COLUMNS,
}


def scan_incoming_folder(folder_path: str, extension: str = ".csv"):
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"The folder {folder_path} does not exist")

    if not folder.is_dir():
        raise NotADirectoryError(f"{folder_path} is not a directory")

    for file in folder.rglob(f"*{extension}"):
        yield file


def get_expected_schema(file_name: str):
    file_name = file_name.lower()

    if "order_items" in file_name:
        return SCHEMA_MAP["order_items"]

    if "customers" in file_name:
        return SCHEMA_MAP["customers"]

    if "payments" in file_name:
        return SCHEMA_MAP["payments"]

    if "orders" in file_name:
        return SCHEMA_MAP["orders"]

    return None


def validate_schema(df: pd.DataFrame, expected_columns: list[str]) -> tuple[bool, str]:
    actual_columns = df.columns.tolist()

    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]

    if missing_columns:
        return False, f"Missing columns: {missing_columns}"

    if extra_columns:
        return False, f"Extra columns: {extra_columns}"

    return True, "Schema validation passed"


def validate_data(df: pd.DataFrame, file_name: str) -> tuple[bool, str]:
    file_name = file_name.lower()

    if "customers" in file_name:
        if df["customer_id"].isnull().any():
            return False, "customer_id has null values"
        if df["customer_unique_id"].isnull().any():
            return False, "customer_unique_id has null values"
        if df["customer_id"].duplicated().any():
            return False, "duplicate customer_id found"

    elif "orders" in file_name and "order_items" not in file_name:
        if df["order_id"].isnull().any():
            return False, "order_id has null values"
        if df["customer_id"].isnull().any():
            return False, "customer_id has null values"
        if df["order_id"].duplicated().any():
            return False, "duplicate order_id found"

    elif "order_items" in file_name:
        if df["order_id"].isnull().any():
            return False, "order_id has null values"
        if df["order_item_id"].isnull().any():
            return False, "order_item_id has null values"
        if df[["order_id", "order_item_id"]].duplicated().any():
            return False, "duplicate order_id + order_item_id found"
        if (df["price"] < 0).any():
            return False, "price has negative values"
        if (df["freight_value"] < 0).any():
            return False, "freight_value has negative values"

    elif "payments" in file_name:
        if df["order_id"].isnull().any():
            return False, "order_id has null values"
        if df["payment_value"].isnull().any():
            return False, "payment_value has null values"
        if (df["payment_value"] < 0).any():
            return False, "payment_value has negative values"

    return True, "Data validation passed"


def move_file(file: Path, target_folder: str):
    target_path = Path(target_folder)
    target_path.mkdir(parents=True, exist_ok=True)

    destination = target_path / file.name

    if destination.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination = target_path / f"{file.stem}_{timestamp}{file.suffix}"

    shutil.move(str(file), str(destination))
    return destination


def write_audit_log(
    audit_file: str,
    file_name: str,
    status: str,
    reason: str,
    destination: str,
    row_count: int
):
    audit_path = Path(audit_file)
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = audit_path.exists()

    with open(audit_path, "a", newline="") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "timestamp",
                "file_name",
                "status",
                "reason",
                "destination",
                "row_count",
            ])

        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            file_name,
            status,
            reason,
            destination,
            row_count,
        ])


def reject_file(file: Path, rejected_folder: str, reason: str, audit_file: str, row_count: int):
    try:
        destination = move_file(file, rejected_folder)
    except Exception as e:
        logger.exception("Failed to move file to rejected folder: %s", file.name)
        destination = f"MOVE_FAILED: {e}"

    write_audit_log(
        audit_file=audit_file,
        file_name=file.name,
        status="INVALID",
        reason=reason,
        destination=str(destination),
        row_count=row_count,
    )

    return destination


def process_files(
    incoming_folder: str,
    processed_folder: str,
    rejected_folder: str,
    audit_file: str
):
    valid_files = []
    invalid_files = []

    for file in scan_incoming_folder(incoming_folder):
        logger.info("Processing file: %s", file.name)

        expected_columns = get_expected_schema(file.name)

        if expected_columns is None:
            message = "Unknown file type"
            destination = reject_file(file, rejected_folder, message, audit_file, 0)

            logger.error("INVALID file: %s - %s", file.name, message)
            invalid_files.append((file.name, message, str(destination)))
            continue

        try:
            total_rows = 0
            is_first_chunk = True
            file_invalid = False

            for chunk in pd.read_csv(file, chunksize=10000):
                total_rows += len(chunk)

                if is_first_chunk:
                    is_schema_valid, schema_message = validate_schema(
                        chunk,
                        expected_columns
                    )

                    if not is_schema_valid:
                        destination = reject_file(
                            file,
                            rejected_folder,
                            schema_message,
                            audit_file,
                            total_rows,
                        )

                        logger.error(
                            "INVALID schema: %s - %s",
                            file.name,
                            schema_message
                        )

                        invalid_files.append(
                            (file.name, schema_message, str(destination))
                        )

                        file_invalid = True
                        break

                    is_first_chunk = False

                is_data_valid, data_message = validate_data(chunk, file.name)

                if not is_data_valid:
                    destination = reject_file(
                        file,
                        rejected_folder,
                        data_message,
                        audit_file,
                        total_rows,
                    )

                    logger.error(
                        "INVALID data: %s - %s",
                        file.name,
                        data_message
                    )

                    invalid_files.append(
                        (file.name, data_message, str(destination))
                    )

                    file_invalid = True
                    break

            if file_invalid:
                continue

            bucket_name = "shaik-olist-bucket"
            s3_key = f"bronze/{file.name}"

            try:
                upload_to_s3(file, bucket_name, s3_key)
                destination = move_file(file, processed_folder)

                logger.info(
                    "VALID file: %s - uploaded to s3://%s/%s and moved to %s | rows=%s",
                    file.name,
                    bucket_name,
                    s3_key,
                    destination,
                    total_rows,
                )

                valid_files.append((file.name, str(destination)))

                write_audit_log(
                    audit_file,
                    file.name,
                    "VALID",
                    "All validations passed and uploaded to S3",
                    str(destination),
                    total_rows,
                )

            except Exception as e:
                message = f"S3 upload or processed move failed: {e}"
                logger.exception("FAILED after validation: %s", file.name)

                destination = reject_file(
                    file,
                    rejected_folder,
                    message,
                    audit_file,
                    total_rows,
                )

                invalid_files.append((file.name, message, str(destination)))

        except Exception as e:
            message = str(e)
            logger.exception("FAILED to process file: %s", file.name)

            destination = reject_file(
                file,
                rejected_folder,
                message,
                audit_file,
                0,
            )

            invalid_files.append((file.name, message, str(destination)))

    return valid_files, invalid_files


if __name__ == "__main__":
    incoming_folder = "data/incoming"
    processed_folder = "data/processed"
    rejected_folder = "data/rejected"
    audit_file = "data/audit/audit_log.csv"

    valid_files, invalid_files = process_files(
        incoming_folder,
        processed_folder,
        rejected_folder,
        audit_file,
    )

    print("\nValid files:")
    for file_name, destination in valid_files:
        print(f"- {file_name} -> {destination}")

    print("\nInvalid files:")
    for file_name, reason, destination in invalid_files:
        print(f"- {file_name} -> {destination} | Reason: {reason}")