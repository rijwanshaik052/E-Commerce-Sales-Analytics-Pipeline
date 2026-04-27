# import boto3
# from pathlib import Path
# import logging

# logger = logging.getLogger(__name__)

# s3_client = boto3.client("s3")

# def upload_to_s3(file_path: Path, bucket_name: str, s3_key: str):
#     try:
#         s3_client.upload_file(str(file_path), bucket_name, s3_key)
#         logger.info("Uploaded to S3: s3://%s/%s", bucket_name, s3_key)
#     except Exception as e:
#         logger.error("S3 upload failed: %s", e)


import boto3
from pathlib import Path
import logging
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


def upload_to_s3(file_path: Path, bucket_name: str, s3_key: str):
    try:
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        s3_client.upload_file(str(file_path), bucket_name, s3_key)

        logger.info("Uploaded to S3: s3://%s/%s", bucket_name, s3_key)

    except NoCredentialsError as e:
        logger.error("AWS credentials not found: %s", e)
        raise

    except ClientError as e:
        logger.error("AWS S3 client error: %s", e)
        raise

    except Exception as e:
        logger.error("S3 upload failed: %s", e)
        raise