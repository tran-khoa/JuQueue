import logging
import sys
from pathlib import Path
from typing import Dict

from loguru import logger


def format_record(record: Dict) -> str:
    run_id = ""
    if record["extra"].get("run_id") is not None:
        run_id = f"<level>{record['extra']['run_id']}</level> | "

    format_string = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        f"{run_id}"
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>\n"
    )
    return format_string


def setup_logger(log_path: Path, debug: bool):
    log_path.mkdir(exist_ok=True, parents=True)

    log_level = logging.INFO
    if debug:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)

    logger.remove()
    logger.add(sys.stderr, format=format_record, level=log_level, backtrace=True, diagnose=True)
    logger.add(log_path / "juqueue.log",
               format=format_record, rotation="1 day", retention="5 days", compression="gz", level=log_level)
