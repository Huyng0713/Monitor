import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Optional

import logging

from log import log_exception, log_file_issue

LOG_PATTERN = re.compile(
    r"(?P<ip>\S+) \S+ \S+ "
    r"\[(?P<time>[^\]]+)\] "
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r"(?P<status>\d{3}) "
    r"(?P<size>\d+) "
    r'"(?P<referer>[^"]*)" '
    r'"(?P<user_agent>[^"]*)"'
)

TIME_FORMAT = "%d/%b/%Y:%H:%M:%S %z"


@dataclass
class LogEntry:
    ip: str
    time: datetime
    method: str
    path: str
    status: int
    size: int
    referer: str
    user_agent: str


def parse_line(line: str) -> Optional[LogEntry]:
    match = LOG_PATTERN.match(line.strip())
    if not match:
        if line.strip():
            log_file_issue(logging.WARNING, "Failed to parse access log line: sample=%s", line.strip()[:100])
        return None

    try:
        data = match.groupdict()
        return LogEntry(
            ip=data["ip"],
            time=datetime.strptime(data["time"], TIME_FORMAT),
            method=data["method"],
            path=data["path"],
            status=int(data["status"]),
            size=int(data["size"]),
            referer=data["referer"],
            user_agent=data["user_agent"],
        )
    except Exception:
        log_exception("Error creating LogEntry from line: sample=%s", line.strip()[:100])
        return None


def parse_file(filepath: str) -> Iterator[LogEntry]:
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as file_obj:
            for line in file_obj:
                entry = parse_line(line)
                if entry:
                    yield entry
    except FileNotFoundError:
        log_file_issue(logging.ERROR, "Access log file not found: path=%s", filepath)
    except Exception:
        log_exception("Error reading access log file: path=%s", filepath)


def tail_file(filepath: str) -> Iterator[LogEntry]:
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as file_obj:
            file_obj.seek(0, os.SEEK_END)
            while True:
                line = file_obj.readline()
                if line:
                    entry = parse_line(line)
                    if entry:
                        yield entry
                else:
                    import time

                    time.sleep(0.5)
    except FileNotFoundError:
        log_file_issue(logging.ERROR, "Tail source file not found: path=%s", filepath)
    except Exception:
        log_exception("Unexpected error while tailing file: path=%s", filepath)
