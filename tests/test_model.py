from datetime import datetime
from pathlib import Path
import uuid
import pytest
from aggregator.model import File, LogEntry, JavaLogEntry, LogFile, ZipFile

test_uuid: uuid.UUID = uuid.UUID('d525b033-e4ac-4acf-b3ba-219ab974f0c5')


class TestFileModel:

    @pytest.mark.unit
    def test_file_model(self) -> None:
        # Given a class (File)
        # When it is instantiated
        id = uuid.uuid4()
        file: File = File(id=id)

        # Then the object exists
        assert type(file.id) == uuid.UUID
        assert file.path is None
        assert file.name is None
        assert file.filetype is None
        assert file.node is None

    def test_file_model_uuid(self) -> None:
        # Given a class (File)
        # And an id
        id: uuid.UUID = test_uuid
        # When it is instantiated with an id
        file: File = File(id=id)
        # Then the id remains
        assert file.id == id

    def test_file_model_vars(self, tmp_path) -> None:
        # Given a class (File)
        # And some vars
        id: uuid.UUID = test_uuid
        path: Path = tmp_path
        name: Path = Path("file.txt")
        filetype: Path = Path(".txt")
        node: str = "node001"
        
        # When the File is instantiated
        file: File = File(id=id, path=path, name=name, filetype=filetype, node=node)

        # Then the object exists
        assert file.id == id
        assert file.path == path
        assert file.name == name
        assert file.filetype == filetype
        assert file.node == node


class TestZipFileModel(TestFileModel):

    @pytest.mark.unit
    def test_zipfile_model(self) -> None:
        # Given a class (ZipFile)
        # When it is instantiated
        zip_file: ZipFile = ZipFile()

        # Then the object exists & is a zip
        assert zip_file.filetype == "zip"


class TestLogFileModel(TestFileModel):

    @pytest.mark.unit
    def test_logfile_model(self) -> None:
        # Given a class (LogFile)
        # And a source zip
        id: uuid.UUID = test_uuid
        zip_file: ZipFile = ZipFile(id=id)
        # When it is instantiated
        log_file: LogFile = LogFile(source_zip=zip_file)

        # Then the object exists & is a log
        assert log_file.filetype == "log"
        # And the source is the ZipFile
        assert log_file.source_zip.id == zip_file.id


class TestLogEntryModel():

    @pytest.mark.unit
    def test_log_entry_model(self) -> None:
        # Given a class (LogEntry)
        # And a source log_file
        id: uuid.UUID = test_uuid
        zip_file: ZipFile = ZipFile()
        log_file: LogFile = LogFile(id=id, source_zip=zip_file)
        # When it is instantiated
        log_entry: LogEntry = LogEntry(
            source_file=log_file,
            timestamp=datetime.now(),
            message="Message"
            )

        # Then the object exists & is a log
        assert type(log_entry.id) == uuid.UUID
        assert log_entry.source_file.id == id
        assert type(log_entry.source_file.source_zip.id) == uuid.UUID
        assert log_entry.message == "Message" 


class TestJavaLogEntry(TestLogEntryModel):

    @pytest.mark.unit
    def test_javalog_entry(self) -> None:
        # Given a class(JavaLog)
        # And sources
        id: uuid.UUID = test_uuid
        zip_file: ZipFile = ZipFile()
        log_file: LogFile = LogFile(id=id, source_zip=zip_file)
        # When it is instantiated with additional vars
        javalog_entry: JavaLogEntry = JavaLogEntry(
            source_file=log_file,
            timestamp=datetime.now(),
            severity="INFO",
            jvm="jvm 1",
            module="AsyncFileSystem",
            type="Async",
            message="Message",
        )

        # Then the javalogentry exists & is a javalog
        assert type(javalog_entry.id) == uuid.UUID
        assert javalog_entry.source_file.id == id
        assert javalog_entry.severity == "INFO"
        assert javalog_entry.module == "AsyncFileSystem"
        assert javalog_entry.type == "Async" 
