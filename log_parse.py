import re
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Iterator
import os


LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) \S+ \S+ '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d{3}) '
    r'(?P<size>\d+) '
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
        return None
    d = match.groupdict()
    return LogEntry(
        ip=d["ip"],
        time=datetime.strptime(d["time"], TIME_FORMAT),
        method=d["method"],
        path=d["path"],
        status=int(d["status"]),
        size=int(d["size"]),
        referer=d["referer"],
        user_agent=d["user_agent"],
    )

def parse_file(filepath: str) -> Iterator[LogEntry]:
    """Read entire existing log file line by line (no full RAM load)."""
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            entry = parse_line(line)
            if entry:
                yield entry

def tail_file(filepath: str) -> Iterator[LogEntry]:
    """Watch log file for new lines (like tail -f)."""
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        f.seek(0, os.SEEK_END)  
        while True:
            line = f.readline()
            if line:
                entry = parse_line(line)
                if entry:
                    yield entry
            else:
                import time
                time.sleep(0.5)