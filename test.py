# test_parser.py
from log_parse import parse_file
from db import init_db, insert_many

init_db()
entries = list(parse_file("access.log"))
insert_many(entries)
print(f"Inserted {len(entries)} entries into DB")