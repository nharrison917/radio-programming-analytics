import shutil
from datetime import datetime
from scraper.config import DB_PATH
from pathlib import Path
import logging


def create_backup():
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"radio_plays_backup_{timestamp}.db"

    shutil.copy2(DB_PATH, backup_path)

    print(f"Database backup created: {backup_path}")

    return str(backup_path)

def setup_logging(job_name="job"):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{job_name}_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logging.info("Logging initialized.")
    return str(log_file)