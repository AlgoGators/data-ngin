import logging
from logging.handlers import RotatingFileHandler

def setup_logging(log_file: str = "./app/app_logs.log", max_bytes: int = 10**6, backup_count: int = 3) -> None:
    """
    Configures logging for the application, writing logs to both console and a rotating file.

    Args:
        log_file (str): The path to the log file.
        max_bytes (int): The maximum size (in bytes) of the log file before rotation.
        backup_count (int): The number of backup log files to keep.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Log to console
            RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count),  # Log to file
        ],
    )
