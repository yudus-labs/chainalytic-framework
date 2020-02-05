import json
import logging
from pathlib import Path
import os
from chainalytic.common import config


def pretty(d: dict) -> str:
    return json.dumps(d, indent=2, sort_keys=1)


def create_logger(logger_name: str, log_location: str = '', level: int = None):
    if not level:
        level = int(os.environ['LOG_LEVEL']) if 'LOG_LEVEL' in os.environ else logging.WARNING
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    log_path = Path(
        config.get_working_dir(),
        config.CHAINALYTIC_FOLDER,
        'log',
        log_location,
        f'{logger_name}.log',
    )
    log_path.parent.mkdir(parents=1, exist_ok=1)

    fh = logging.FileHandler(log_path.as_posix(), mode='w')
    fh.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s | %(name)s - %(levelname)s | %(message)s', '%m-%d %H:%M:%S'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def get_child_logger(logger_name: str):
    return logging.getLogger(logger_name)
