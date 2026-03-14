from pathlib import Path


def scan_incoming_files(folder_path: str):
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"Folder {folder_path} does not exist")

    files = []

    for file in folder.iterdir():
        if file.is_file():
            files.append(file.name)

    return files


