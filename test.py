from log_parse import parse_file
from db import init_db, insert_many
from log import logger

init_db()
logger.info("Database initialized")

entries = list(parse_file("access.log"))
logger.info(f"Parsed {len(entries)} entries from log file")

insert_many(entries)
logger.info(f"Inserted {len(entries)} entries into DB")