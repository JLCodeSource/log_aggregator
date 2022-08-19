import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, validator

from aggregator import helper

logger: logging.Logger = logging.getLogger(__name__)


class File(BaseModel):
    id: uuid.UUID | None = uuid.uuid4()
    fullpath: Path
    filename: Path | None = None
    extension: Path | None = None
    node: str | None = None
    log_type: str | None = None

    def __init__(self, **data) -> None:
        data["extension"] = Path(os.path.splitext(data["fullpath"])[1])
        data["filename"] = Path(os.path.basename(data["fullpath"]))
        super().__init__(**data)


class ZipFile(File):
    def __init__(self, **data) -> None:
        fullpath: Path = data["fullpath"]
        data["node"] = helper.get_node(fullpath, helper.ZIP_NODE_PATTERN)
        data["log_type"] = helper.get_log_type(fullpath, helper.ZIP_LOGTYPE_PATTERN)
        super().__init__(**data)

    @validator("extension")
    def extension_must_be_zip(cls, v, values) -> Path:
        if v != Path(".zip"):
            fullpath: Path = values["fullpath"]
            err: str = f"ValueError: ZipFile {fullpath} must have .zip extension"
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v


class LogFile(File):
    source_zip: ZipFile
    file_type: str = "log"
    logtype: str | None = None

    @validator("extension")
    def extension_must_be_log(cls, v, values) -> Path:
        if not str(v).startswith(".log"):
            fullpath: Path = values["fullpath"]
            err: str = f"ValueError: LogFile {fullpath} must have .log* extension"
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v


class LogEntry(BaseModel):
    id: uuid.UUID | None = uuid.uuid4()
    source_log: LogFile
    node: str | None = None
    timestamp: datetime
    message: str


class JavaLogEntry(LogEntry):
    severity: str
    jvm: str | None = None
    module: str | None = None
    type: str | None = None
