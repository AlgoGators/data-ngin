import logging
import os
from logging.handlers import RotatingFileHandler

_configured = False


def setup_logging(log_file: str = "logs/data_ngin.log", max_bytes: int = 10**6, backup_count: int = 3) -> None:
    """
    Configures logging for the application, writing logs to both console and a
    rotating file. Idempotent -- safe to call from every entrypoint (orchestrator,
    API server, DAG) without stacking duplicate handlers on repeated imports.

    Args:
        log_file (str): The path to the log file.
        max_bytes (int): The maximum size (in bytes) of the log file before rotation.
        backup_count (int): The number of backup log files to keep.
    """
    global _configured
    if _configured:
        return

    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Log to console
            RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count),  # Log to file
        ],
    )
    _configured = True
