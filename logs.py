import logging


class OneLineExceptionFormatter(logging.Formatter):
    """def formatException(self, exc_info):
    Format an exception so that it prints on a single line

    result = super().formatException(exc_info)
    return repr(result)
    """

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
