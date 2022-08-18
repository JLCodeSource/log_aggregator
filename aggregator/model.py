import uuid
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel


class File(BaseModel):
    id: uuid.UUID | None = uuid.uuid4()
    path: Path | None = None
    name: Path | None = None
    filetype: Path | None = None
    node: str | None = None


class ZipFile(File):
    filetype: Path = Path("zip")


class LogFile(File):
    source_zip: ZipFile
    filetype: Path = Path("log")
    logtype: str | None = None


class LogEntry(BaseModel):
    id: uuid.UUID | None = uuid.uuid4()
    source_file: LogFile
    node: str | None = None
    timestamp: datetime
    message: str


class JavaLogEntry(LogEntry):
    severity: str
    jvm: str | None = None
    module: str | None = None
    type: str | None = None
