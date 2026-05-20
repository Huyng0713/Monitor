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


def iter_entry_batches(entries: Iterable[LogEntry], batch_size: int) -> Iterator[list[LogEntry]]:
    batch: list[LogEntry] = []
    for entry in entries:
        batch.append(entry)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
