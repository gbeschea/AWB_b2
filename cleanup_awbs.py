# cleanup_awbs.py
import os
from pathlib import Path
from datetime import datetime, timedelta
from settings import settings

ARCHIVE_BASE_DIR = Path('awb_archive')
RETENTION_DAYS = settings.ARCHIVE_RETENTION_DAYS

def cleanup_old_files():
    if not ARCHIVE_BASE_DIR.is_dir():
        print(f"Directorul de arhivă '{ARCHIVE_BASE_DIR}' nu există. Ies.")
        return

    print("Pornesc curățenia fișierelor vechi...")
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    
    for day_folder in ARCHIVE_BASE_DIR.iterdir():
        if not day_folder.is_dir():
            continue
        
        try:
            folder_date = datetime.strptime(day_folder.name, '%Y-%m-%d')
            if folder_date < cutoff_date:
                print(f"Șterg directorul vechi: {day_folder}...")
                for file_in_folder in day_folder.iterdir():
                    try:
                        file_in_folder.unlink()
                    except OSError as e:
                        print(f"Eroare la ștergerea fișierului {file_in_folder}: {e}")
                try:
                    day_folder.rmdir()
                except OSError as e:
                    print(f"Eroare la ștergerea directorului {day_folder}: {e}")
        except ValueError:
            print(f"Ignor directorul cu nume invalid: {day_folder.name}")

    print("Curățenia s-a încheiat.")

if __name__ == "__main__":
    cleanup_old_files()