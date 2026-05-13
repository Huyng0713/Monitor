from dataclasses import dataclass
from typing import Iterable, Iterator, Protocol

from log_parse import LogEntry, parse_file


class LogSource(Protocol):
    name: str

    def read_entries(self) -> Iterator[LogEntry]:
        ...


@dataclass
class FileLogSource:
    name: str
    filepath: str

    def read_entries(self) -> Iterator[LogEntry]:
        return parse_file(self.filepath)


def collect_entries(source: LogSource) -> list[LogEntry]:
    return list(source.read_entries())


def collect_entries_from_sources(sources: Iterable[LogSource]) -> list[LogEntry]:
    entries: list[LogEntry] = []
    for source in sources:
        entries.extend(source.read_entries())
    return entries
