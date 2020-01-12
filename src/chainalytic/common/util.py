import json
import logging
from pathlib import Path
from chainalytic.common import config


def pretty(d: dict) -> str:
    return json.dumps(d, indent=2, sort_keys=1)


def create_logger(logger_name: str, log_location: str = '', level: int = logging.DEBUG):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    fh = logging.FileHandler(
        Path(
            config.get_working_dir(),
            config.CHAINALYTIC_FOLDER,
            'log',
            log_location,
            f'{logger_name}.log',
        ).as_posix()
    )
    fh.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def get_child_logger(logger_name: str):
    return logging.getLogger(logger_name)
