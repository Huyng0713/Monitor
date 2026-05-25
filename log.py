import logging
import os
import asyncio
import traceback
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


def log_to_db(logger_name: str, level_name: str, message: str, traceback_str: str | None = None):
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            from db import insert_system_log
            loop.create_task(insert_system_log(logger_name, level_name, message, traceback_str))
    except RuntimeError:
        pass
    except Exception:
        pass


def _safe_format(message: str, args) -> str:
    if not args:
        return message
    try:
        return message % args
    except Exception:
        return f"{message} (args: {args})"


def log_activity(message: str, *args):
    app_logger.info(message, *args)
    log_to_db(APP_LOGGER_NAME, "INFO", _safe_format(message, args))


def log_error(message: str, *args):
    error_logger.error(message, *args)
    log_to_db(ERROR_LOGGER_NAME, "ERROR", _safe_format(message, args))


def log_exception(message: str, *args):
    error_logger.exception(message, *args)
    tb_str = traceback.format_exc()
    log_to_db(ERROR_LOGGER_NAME, "ERROR", _safe_format(message, args), tb_str)


def log_file_issue(level: int, message: str, *args):
    file_logger.log(level, message, *args)
    level_name = logging.getLevelName(level)
    log_to_db(FILE_LOGGER_NAME, level_name, _safe_format(message, args))


def get_log_paths() -> Dict[str, str]:
    return {
        "app": APP_LOG_PATH,
        "error": ERROR_LOG_PATH,
        "file": FILE_LOG_PATH,
    }
