from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

import os

INCOMING_DATA_FOLDER = os.getenv("INCOMING_DATA_FOLDER")

if not INCOMING_DATA_FOLDER:
    raise ValueError("INCOMING_DATA_FOLDER is not set")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
ENV = os.getenv("ENV", "development")   
