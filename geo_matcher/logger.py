import logging
import os
import sys

_LEVEL_COLORS: dict[int, str] = {
    logging.DEBUG: "\033[36m",  # cyan
    logging.INFO: "\033[32m",  # green
    logging.WARNING: "\033[33m",  # yellow
    logging.ERROR: "\033[31m",  # red
    logging.CRITICAL: "\033[1;31m",  # bold red
}
_RESET = "\033[0m"


class _ColorFormatter(logging.Formatter):
    """Colors ``levelname`` by level when writing to a TTY; plain text otherwise."""

    def __init__(self, *args: object, use_color: bool, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        padded = f"{original_levelname:<8}"
        color = _LEVEL_COLORS.get(record.levelno) if self._use_color else None
        record.levelname = f"{color}{padded}{_RESET}" if color else padded
        try:
            return super().format(record)
        finally:
            record.levelname = original_levelname


def _get_log_level() -> int:
    """Set the logging level"""
    level_name = os.getenv("GEO_MATCHER_DEBUG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, None)

    if not isinstance(level, int):
        raise ValueError(
            f"Invalid GEO_MATCHER_DEBUG_LEVEL={level_name!r}. "
            "Use one of: DEBUG, INFO, WARNING, ERROR, CRITICAL."
        )

    return level


def get_logger(name: str = "geo_matcher") -> logging.Logger:
    """Create logger"""
    logger: logging.Logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = _get_log_level()

    logger.setLevel(level)

    handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    use_color = (
        hasattr(handler.stream, "isatty")
        and handler.stream.isatty()
        and os.getenv("NO_COLOR") is None
        and os.getenv("GEO_MATCHER_NO_COLOR") is None
    )
    handler.setFormatter(
        _ColorFormatter(
            "%(asctime)s  %(levelname)s  %(message)s",
            datefmt="%H:%M:%S",
            use_color=use_color,
        )
    )
    logger.addHandler(handler)
    return logger
