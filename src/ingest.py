from pathlib import Path
import logging
import shutil
import pandas as pd

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


def move_file(file: Path, target_folder: str):
    target_path = Path(target_folder)
    target_path.mkdir(parents=True, exist_ok=True)

    destination = target_path / file.name
    shutil.move(str(file), str(destination))

    return destination


def process_files(incoming_folder: str, processed_folder: str, rejected_folder: str):
    valid_files = []
    invalid_files = []

    for file in scan_incoming_folder(incoming_folder):
        logger.info("Processing file: %s", file.name)

        expected_columns = get_expected_schema(file.name)

        if expected_columns is None:
            message = "Unknown file type"
            logger.error("INVALID file: %s - %s", file.name, message)

            destination = move_file(file, rejected_folder)
            invalid_files.append((file.name, message, str(destination)))
            continue

        try:
            df = pd.read_csv(file)

            is_valid, message = validate_schema(df, expected_columns)

            if is_valid:
                destination = move_file(file, processed_folder)
                logger.info("VALID file: %s - moved to %s", file.name, destination)
                valid_files.append((file.name, str(destination)))
            else:
                destination = move_file(file, rejected_folder)
                logger.error("INVALID file: %s - %s", file.name, message)
                invalid_files.append((file.name, message, str(destination)))

        except Exception as e:
            message = str(e)
            logger.exception("FAILED to process file: %s", file.name)

            destination = move_file(file, rejected_folder)
            invalid_files.append((file.name, message, str(destination)))

    return valid_files, invalid_files


if __name__ == "__main__":
    incoming_folder = "data/incoming"
    processed_folder = "data/processed"
    rejected_folder = "data/rejected"

    valid_files, invalid_files = process_files(
        incoming_folder,
        processed_folder,
        rejected_folder
    )

    print("\nValid files:")
    for file_name, destination in valid_files:
        print(f"- {file_name} -> {destination}")

    print("\nInvalid files:")
    for file_name, reason, destination in invalid_files:
        print(f"- {file_name} -> {destination} | Reason: {reason}")