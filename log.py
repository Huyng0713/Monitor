import logging
import os
import asyncio
import time
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
LOG_ERRORS_TO_DB = os.getenv("LOG_ERRORS_TO_DB", "1") == "1"
DB_LOG_COOLDOWN_SECONDS = int(os.getenv("DB_LOG_COOLDOWN_SECONDS", "60"))

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

_db_log_suspended_until = 0.0
_db_log_tasks = set()


def _is_database_log_message(message: str) -> bool:
    lowered = message.lower()
    return (
        "database " in lowered
        or "cached_stat" in lowered
        or "system logs fetch" in lowered
    )


def log_to_db(logger_name: str, level_name: str, message: str, traceback_str: str | None = None):
    if not LOG_ERRORS_TO_DB or level_name != "ERROR":
        return
    if _is_database_log_message(message):
        return
    if time.monotonic() < _db_log_suspended_until:
        return
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            from db import insert_system_log
            task = loop.create_task(insert_system_log(logger_name, level_name, message, traceback_str))
            _db_log_tasks.add(task)
            task.add_done_callback(_handle_log_to_db_result)
    except RuntimeError:
        pass
    except Exception:
        pass


def _handle_log_to_db_result(task: asyncio.Task):
    global _db_log_suspended_until

    _db_log_tasks.discard(task)
    try:
        result = task.result()
    except asyncio.CancelledError:
        result = False
    except Exception:
        result = False

    if result is False:
        _db_log_suspended_until = time.monotonic() + DB_LOG_COOLDOWN_SECONDS


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


def _add_stdout_handler(logger: logging.Logger, level: int) -> None:
    """Add StreamHandler to stdout for Vercel logging collection."""
    import sys
    if any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
        for h in logger.handlers
    ):
        return  # Already configured, don't duplicate
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)


IS_VERCEL = os.getenv("VERCEL") == "1"
if IS_VERCEL:
    _add_stdout_handler(app_logger, logging.INFO)
    _add_stdout_handler(error_logger, logging.ERROR)
    _add_stdout_handler(file_logger, logging.WARNING)
