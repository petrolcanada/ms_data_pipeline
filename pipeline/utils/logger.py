"""
Logging Configuration
Provides structured logging with optional name-sanitization for obfuscated runs.
"""
import logging
import json
import sys
from typing import Optional, Dict


class SanitizingFilter(logging.Filter):
    """Replace real table names with obfuscated IDs in log records.

    Only active when a name mapping has been registered via
    ``register_name_mapping``. At INFO level and below, matched names are
    replaced; DEBUG keeps original names for developer use.
    """

    _mapping: Dict[str, str] = {}

    @classmethod
    def register_name_mapping(cls, real_to_obfuscated: Dict[str, str]):
        cls._mapping = dict(real_to_obfuscated)

    def filter(self, record: logging.LogRecord) -> bool:
        if self._mapping and record.levelno <= logging.INFO:
            msg = record.getMessage()
            for real_name, obf_id in self._mapping.items():
                msg = msg.replace(real_name, f"[{obf_id}]")
            record.msg = msg
            record.args = None
        return True


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects for machine parsing."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup_logging(structured: bool = False):
    """Set up application logging.

    Args:
        structured: If True, use JSON formatter on stdout for machine parsing.
    """
    from pipeline.config.settings import get_settings

    settings = get_settings()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    if root.handlers:
        root.handlers.clear()

    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler("pipeline.log")

    if structured:
        fmt = JsonFormatter()
    else:
        fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    stdout_handler.setFormatter(fmt)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )

    root.addHandler(stdout_handler)
    root.addHandler(file_handler)

    sanitizer = SanitizingFilter()
    root.addFilter(sanitizer)

    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get logger instance for a module."""
    return logging.getLogger(name)
