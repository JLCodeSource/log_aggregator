"""
Module Name: logs.py 
Created: 2022-07-24
Creator: JL
Change Log: Initial
Summary: logs configures logging & makes exceptions 1 line
Functions: configureLogging
Classes: OneLineExceptionFormatter
"""
import logging


class OneLineExceptionFormatter(logging.Formatter):
    def format(self, record):
        result = super().format(record)
        if record.exc_text:
            result = result.replace("\n", " ; ")
            result = result.replace("  ", "")
            trace = result.split(" ; Traceback")
            if len(trace) > 0:
                result = trace
                result = result[0]
        return result


def configureLogging():
    handler = logging.StreamHandler()
    format = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    formatter = OneLineExceptionFormatter(format)
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
