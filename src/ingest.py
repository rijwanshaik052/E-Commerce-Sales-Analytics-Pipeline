from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scan_incoming_folder(folder_path:str, extension: str = ".csv"):
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"the folder {folder_path} does not exist")

    if not folder.is_dir():
        raise NotADirectoryError(f"the path {folder_path} is not a directory")
    for file in folder.rglob(f"*{extension}"):
        yield file

for file in scan_incoming_folder("data/incoming"):
    logger.info(f"Found file: {file}")
    