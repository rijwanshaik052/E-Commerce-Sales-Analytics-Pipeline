from pathlib import Path
import os   
from config.settings import INCOMING_DATA_FOLDER



def scan_incoming_files():
    folder = Path(INCOMING_DATA_FOLDER)
    if not folder.exists():
        raise ValueError(f"Incoming data folder {INCOMING_DATA_FOLDER} does not exist")
    files = list(folder.glob("*.csv"))
    return files


files = scan_incoming_files()
print(files)