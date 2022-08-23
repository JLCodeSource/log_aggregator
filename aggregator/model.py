import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, root_validator, validator

from aggregator import helper

logger: logging.Logger = logging.getLogger(__name__)


class File(BaseModel):
    id: uuid.UUID | None = uuid.uuid4()
    full_path: Path
    filename: Path | None = None
    extension: Path | None = None
    node: str | None = None
    log_type: str | None = None

    def __init__(self, **data) -> None:
        data["extension"] = Path(os.path.splitext(data["full_path"])[1])
        data["filename"] = Path(os.path.basename(data["full_path"]))
        super().__init__(**data)

    class Config:
        extra: str = "forbid"


class ZipFile(File):
    def __init__(self, **data) -> None:
        full_path: Path = data["full_path"]
        data["node"] = helper.get_node(full_path, helper.ZIP_NODE_PATTERN)
        data["log_type"] = helper.get_log_type(full_path, helper.ZIP_LOGTYPE_PATTERN)
        super().__init__(**data)

    @validator("extension")
    def extension_must_be_zip(cls, v, values) -> Path:
        if v != Path(".zip"):
            full_path: Path = values["full_path"]
            id: uuid.UUID = values["id"]
            err: str = (
                f"ValueError: ZipFile:{id} at {full_path} must have .zip extension"
            )
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v

    @validator("node")
    def node_must_not_be_none_or_empty(cls, v, values) -> str:
        if v is None or v == "":
            full_path: Path = values["full_path"]
            id: uuid.UUID = values["id"]
            err: str = f"ValueError: ZipFile:{id} at {full_path} must have node value"
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v

    @validator("log_type")
    def log_type_must_not_be_none_or_empty(cls, v, values) -> str:
        if v is None or v == "":
            full_path: Path = values["full_path"]
            id: uuid.UUID = values["id"]
            err: str = (
                f"ValueError: ZipFile:{id} at {full_path} must have log_type value"
            )
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v


class LogFile(File):
    source_zip: ZipFile

    def __init__(self, **data) -> None:
        full_path: Path = data["full_path"]
        data["node"] = helper.get_node(full_path, helper.LOG_NODE_PATTERN)
        data["log_type"] = helper.get_log_type(full_path, helper.LOG_LOGTYPE_PATTERN)
        super().__init__(**data)

    @validator("source_zip")
    def source_zip_must_exist(cls, v, values) -> Path:
        full_path: Path = values["full_path"]
        id: uuid.UUID = values["id"]
        if v is None:
            err: str = f"ValueError: LogFile:{id} at {full_path} must have ZipFile"
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v

    # TODO: Add functionality to work with raw log files

    @validator("extension")
    def extension_must_be_log(cls, v, values) -> Path:
        if not str(v).startswith(".log"):
            full_path: Path = values["full_path"]
            id: uuid.UUID = values["id"]
            err: str = (
                f"ValueError: LogFile:{id} at {full_path} must have .log* extension"
            )
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v

    @validator("node")
    def node_must_not_be_none_empty_or_not_match_zip(cls, v, values) -> str:
        full_path: Path = values["full_path"]
        id: uuid.UUID = values["id"]
        if v is None or v == "":
            err: str = f"ValueError: LogFile:{id} at {full_path} must have node value"
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v

    @validator("log_type")
    def log_type_must_not_be_none_or_empty(cls, v, values) -> str:
        if v is None or v == "":
            full_path: Path = values["full_path"]
            id: uuid.UUID = values["id"]
            err: str = (
                f"ValueError: LogFile:{id} at {full_path} must have log_type value"
            )
            logging.error(f"{err}")
            raise ValueError(f"{err}")
        return v

    @root_validator
    def check_node_and_log_type_value_matches_zip_file(cls, values):
        full_path: Path = values["full_path"]
        keys: tuple[str, str, str] = ("node", "log_type", "source_zip")
        if not all(key in values for key in keys):
            raise ValueError()
        else:
            log_id: uuid.UUID = values["id"]
            node: str = values["node"]
            log_type: str = values["log_type"]
            zip_node: str = getattr(values["source_zip"], "node")
            zip_log_type: str = getattr(values["source_zip"], "log_type")
            zip_id: uuid.UUID = getattr(values["source_zip"], "id")

        has_value_erred: bool = False
        if node != zip_node:
            err: str = f"ValueError: LogFile:{log_id} at {full_path} node value must match ZipFile {zip_id} node value"
            logging.error(f"{err}")
            has_value_erred = True
        if log_type != zip_log_type:
            err: str = f"ValueError: LogFile:{log_id} at {full_path} log_type value must match ZipFile {zip_id} log_type value"
            logging.error(f"{err}")
            has_value_erred = True
        if has_value_erred:
            raise ValueError("ValueError: See prior logs")
            # TODO: Make this mechanism cleaner
        return values


class LogEntry(BaseModel):
    id: uuid.UUID | None = uuid.uuid4()
    source_log: LogFile
    node: str | None = None
    timestamp: datetime
    message: str

    class Config:
        extra: str = "forbid"


class JavaLogEntry(LogEntry):
    severity: str
    jvm: str | None = None
    module: str | None = None
    type: str | None = None
