import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from aggregator.model import File, JavaLogEntry, LogEntry, LogFile, ZipFile

test_uuid: uuid.UUID = uuid.UUID("d525b033-e4ac-4acf-b3ba-219ab974f0c5")
module_name: str = "aggregator.model"
zip_filename: str = "GBLogs_node001_apiservice_1234567890123.zip"


@pytest.helpers.register  # type: ignore
def get_file(
    T: type,
    fullpath: Path,
    id: uuid.UUID | None = None,
    source_zip: ZipFile | None = None,
) -> File:

    if T == ZipFile:
        file: File = ZipFile(id=id, fullpath=fullpath)
    elif T == LogFile:
        assert source_zip is not None
        file = LogFile(id=id, fullpath=fullpath, source_zip=source_zip)
    else:
        raise ValueError("Wrong file type")
    return file


class TestFileModel:
    @pytest.mark.unit
    def test_file_model(self) -> None:
        # Given a class (File)
        # When it is instantiated
        id = uuid.uuid4()
        fullpath: Path = Path("filename.txt")
        file: File = File(id=id, fullpath=fullpath)

        # Then the object exists
        assert type(file.id) == uuid.UUID
        assert file.fullpath == Path("filename.txt")
        assert file.node is None
        assert file.log_type is None

    def test_file_model_uuid(self) -> None:
        # Given a class (File)
        # And an id
        id: uuid.UUID = test_uuid
        fullpath: Path = Path("filename.txt")
        # When it is instantiated with an id
        file: File = File(id=id, fullpath=fullpath)
        # Then the id remains
        assert file.id == id

    def test_file_model_node(self, tmp_path) -> None:
        # Given a class (File)
        # And some vars
        id: uuid.UUID = test_uuid
        filename: Path = Path("filename.txt")
        fullpath: Path = Path(os.path.join(tmp_path, filename))
        node: str = "node001"

        # When the File is instantiated
        file: File = File(id=id, fullpath=fullpath, node=node)

        # Then the object exists
        assert file.id == id
        assert file.fullpath == fullpath
        assert file.filename == filename
        assert file.extension == Path(os.path.splitext(filename)[1])
        assert file.node == node

    def test_file_model_path_variants(self, tmp_path) -> None:
        # Given a class (File)
        # And some vars
        id: uuid.UUID = test_uuid
        fullpath: Path = Path(os.path.join(tmp_path, "filename.txt"))
        node: str = "node001"

        # When the File is instantiated
        file: File = File(id=id, fullpath=fullpath, node=node)

        # Then the object exists
        assert file.id == id
        assert file.fullpath == fullpath
        assert Path(os.path.dirname(file.fullpath)) == tmp_path
        assert Path(os.path.basename(file.fullpath)) == Path("filename.txt")
        assert Path(os.path.splitext(file.fullpath)[1]) == Path(".txt")


class TestZipFileModel(TestFileModel):
    @pytest.mark.parametrize(
        "filename, node, log_type",
        [
            ("GBLogs_node001_apiservice_1234567890123.zip", "node001", "apiservice"),
            (
                "GBLogs_node001.domain.tld_apiservice_1234567890123.zip",
                "node001",
                "apiservice",
            ),
        ],
    )
    @pytest.mark.unit
    def test_zipfile_model(
        self, tmp_path: Path, filename: str, node: str, log_type: str
    ) -> None:
        # Given a class (ZipFile)
        # And an example zip_file name
        fullpath: Path = Path(os.path.join(tmp_path, filename))

        # When it is instantiated
        zip_file: ZipFile = ZipFile(fullpath=fullpath)

        # Then the extension, node & log_type are generated
        assert zip_file.extension == Path(".zip")
        assert zip_file.node == node
        assert zip_file.log_type == log_type

    @pytest.mark.parametrize(
        "fullpath, errs",
        [
            (
                Path("/tmp/path/out/not_a_zip.txt"),
                (
                    "ValueError: ZipFile /tmp/path/out/not_a_zip.txt must have .zip extension",
                    "ValueError: ZipFile /tmp/path/out/not_a_zip.txt must have node value",
                    "ValueError: ZipFile /tmp/path/out/not_a_zip.txt must have log_type value",
                ),
            ),
            (
                Path("/tmp/path/out/GBLogs1234567890123.zip"),
                (
                    "ValueError: ZipFile /tmp/path/out/GBLogs1234567890123.zip must have node value",
                    "ValueError: ZipFile /tmp/path/out/GBLogs1234567890123.zip must have log_type value",
                ),
            ),
        ],
    )
    @pytest.mark.unit
    def test_zipfile_validators(
        self, fullpath: Path, errs: tuple[str], logger: pytest.LogCaptureFixture
    ) -> None:
        # Given a class (ZipFile)
        # And a fullpath
        # And a tuple of errs

        # When the file is instantiated
        # Then it raises a Value error
        with pytest.raises(ValueError):
            ZipFile(fullpath=fullpath)

        # And it logs it
        msgs: list[str] = []
        _, _, msgs = pytest.helpers.log_recorder(logger.record_tuples)  # type: ignore
        for i in range(len(errs)):
            assert any(errs[i] == msg for msg in msgs)


class TestLogFileModel(TestFileModel):
    @pytest.mark.parametrize(
        "fullpath, node, log_type",
        [
            ("out/node001/apiservice/apiservice.log", "node001", "apiservice"),
            (
                "out/node001.domain.tld/apiservice/apiservice.log",
                "node001",
                "apiservice",
            ),
            ("out/node001/apiservice/apiservice.log4", "node001", "apiservice"),
        ],
    )
    @pytest.mark.unit
    def test_logfile_log_node_and_log_type(
        self, tmp_path: Path, fullpath: Path, node: str, log_type: str
    ) -> None:
        # Given a class (ZipFile)
        # And a source zip_file name
        zip_path: Path = Path(os.path.join(tmp_path, zip_filename))
        zip_file: ZipFile = ZipFile(fullpath=zip_path)
        # And a log file path
        path: Path = Path(os.path.join(tmp_path, fullpath))

        # When it is instantiated
        log_file: LogFile = LogFile(fullpath=path, source_zip=zip_file)

        # Then the object exists & is a log
        assert str(log_file.extension).startswith(".log")
        # And the source is the ZipFile
        assert log_file.source_zip.id == zip_file.id
        # And the node & log_type are generated
        assert log_file.node == node
        assert log_file.log_type == log_type

    @pytest.mark.unit
    def test_logfile_validator(self, tmp_path, logger) -> None:
        # Given a class (LogFile)
        # And a source zip
        id: uuid.UUID = test_uuid
        fullpath: Path = Path(os.path.join(tmp_path, zip_filename))
        zip_file: ZipFile = ZipFile(id=id, fullpath=fullpath)

        # And a non_log
        fullpath = Path(os.path.join(tmp_path, "not_a_log.txt"))

        # When it is instantiated with the non-zip
        # Then it raises a Value error
        with pytest.raises(ValueError):
            LogFile(source_zip=zip_file, fullpath=fullpath)

        # And it logs it
        lvls: list[int]
        msgs: list[str]
        _, lvls, msgs = pytest.helpers.log_recorder(  # type: ignore
            logger.record_tuples
        )
        assert any(lvl == logging.WARNING for lvl in lvls)
        assert any(lvl == logging.ERROR for lvl in lvls)
        assert any(msg.startswith("Wrong filename structure") for msg in msgs)
        assert any(
            msg == f"ValueError: LogFile {fullpath} must have .log* extension"
            for msg in msgs
        )

    @pytest.mark.unit
    def test_logfile_validator_logN(self, tmp_path) -> None:
        # Given a class (LogFile)
        # And a source zip
        id: uuid.UUID = test_uuid
        fullpath: Path = Path(os.path.join(tmp_path, zip_filename))
        zip_file: ZipFile = ZipFile(id=id, fullpath=fullpath)

        # And a .log5
        fullpath = Path(os.path.join(tmp_path, "logfile.log5"))

        # When it creates the LogFile
        log_file: LogFile = LogFile(source_zip=zip_file, fullpath=fullpath)

        # Then it creates the log
        assert log_file.extension == Path(".log5")


class TestLogEntryModel:
    @pytest.mark.unit
    def test_log_entry_model(self, tmp_path) -> None:
        # Given a class (LogEntry)
        # And source log and zip_files
        id: uuid.UUID = test_uuid
        fullpath: Path = Path(os.path.join(tmp_path, zip_filename))
        zip_file: ZipFile = ZipFile(fullpath=fullpath)
        fullpath = Path(os.path.join(tmp_path, "logfile.log"))
        log_file: LogFile = LogFile(id=id, fullpath=fullpath, source_zip=zip_file)
        # When it is instantiated
        log_entry: LogEntry = LogEntry(
            source_log=log_file, timestamp=datetime.now(), message="Message"
        )

        # Then the object exists & is a log
        assert type(log_entry.id) == uuid.UUID
        assert log_entry.source_log.id == id
        assert type(log_entry.source_log.source_zip.id) == uuid.UUID
        assert log_entry.message == "Message"


class TestJavaLogEntry(TestLogEntryModel):
    @pytest.mark.unit
    def test_javalog_entry(self, tmp_path) -> None:
        # Given a class(JavaLog)
        # And sources
        id: uuid.UUID = test_uuid
        fullpath: Path = Path(os.path.join(tmp_path, zip_filename))
        zip_file: ZipFile = ZipFile(fullpath=fullpath)
        fullpath = Path(os.path.join(tmp_path, "logfile.log"))
        log_file: LogFile = LogFile(id=id, fullpath=fullpath, source_zip=zip_file)
        # When it is instantiated with additional vars
        javalog_entry: JavaLogEntry = JavaLogEntry(
            source_log=log_file,
            timestamp=datetime.now(),
            severity="INFO",
            jvm="jvm 1",
            module="AsyncFileSystem",
            type="Async",
            message="Message",
        )

        # Then the javalogentry exists & is a javalog
        assert type(javalog_entry.id) == uuid.UUID
        assert javalog_entry.source_log.id == id
        assert javalog_entry.severity == "INFO"
        assert javalog_entry.module == "AsyncFileSystem"
        assert javalog_entry.type == "Async"
