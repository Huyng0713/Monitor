import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Dict

from env import load_dotenv
from runtime_paths import ensure_data_dir

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(ensure_data_dir(), "logs")
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 3
DEFAULT_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

APP_LOG_PATH = os.path.join(LOG_DIR, "app.log")
ERROR_LOG_PATH = os.path.join(LOG_DIR, "error.log")
FILE_LOG_PATH = os.path.join(LOG_DIR, "file.log")

APP_LOGGER_NAME = "nginx-monitor.app"
ERROR_LOGGER_NAME = "nginx-monitor.error"
FILE_LOGGER_NAME = "nginx-monitor.file"


def _ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)


def _build_handler(path: str, level: int) -> RotatingFileHandler:
    handler = RotatingFileHandler(
        path,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    return handler


def _configure_logger(name: str, level: int, path: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    _ensure_log_dir()
    handler = _build_handler(path, level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


app_logger = _configure_logger(
    APP_LOGGER_NAME,
    getattr(logging, DEFAULT_LEVEL, logging.INFO),
    APP_LOG_PATH,
)
error_logger = _configure_logger(ERROR_LOGGER_NAME, logging.ERROR, ERROR_LOG_PATH)
file_logger = _configure_logger(FILE_LOGGER_NAME, logging.INFO, FILE_LOG_PATH)


def log_activity(message: str, *args):
    app_logger.info(message, *args)


def log_error(message: str, *args):
    error_logger.error(message, *args)


def log_exception(message: str, *args):
    error_logger.exception(message, *args)


def log_file_issue(level: int, message: str, *args):
    file_logger.log(level, message, *args)


def get_log_paths() -> Dict[str, str]:
    return {
        "app": APP_LOG_PATH,
        "error": ERROR_LOG_PATH,
        "file": FILE_LOG_PATH,
    }
