import logging
import os
import sys


def _get_log_level() -> int:
    level_name = os.getenv("USRN_MATCHER_DEBUG_LEVEL", "DEBUG").upper()
    level = getattr(logging, level_name, None)

    if not isinstance(level, int):
        raise ValueError(
            f"Invalid USRN_MATCHER_DEBUG_LEVEL={level_name!r}. "
            "Use one of: DEBUG, INFO, WARNING, ERROR, CRITICAL."
        )

    return level


def get_logger(name: str = "usrn_matcher") -> logging.Logger:
    logger: logging.Logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = _get_log_level()

    logger.setLevel(level)

    handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S"
        )
    )
    logger.addHandler(handler)
    return logger
