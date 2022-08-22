"""
Module Name: logs.py
Created: 2022-07-24
Creator: JL
Change Log: 2022-07-26 - added environment settings
Summary: logs configures logging & makes exceptions 1 line
Functions: configureLogging
Classes: OneLineExceptionFormatter
"""
import logging
from typing import TextIO

from aggregator.config import Settings, get_settings


class OneLineExceptionFormatter(logging.Formatter):
    def format(self, record) -> str:
        result: str = super().format(record)
        if record.exc_text:
            result = result.replace("\n", " ; ")
            result = result.replace("  ", "")
            trace: list[str] = result.split(" ; Traceback")
            if len(trace) > 0:
                result = trace[0]
        return result


def configure_logging() -> None:
    handler: logging.StreamHandler[TextIO] = logging.StreamHandler()
    format: str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    formatter: OneLineExceptionFormatter = OneLineExceptionFormatter(format)
    handler.setFormatter(formatter)
    logger: logging.Logger = logging.getLogger()
    settings: Settings = get_settings()
    logger.setLevel(settings.log_level)
    logger.addHandler(handler)
