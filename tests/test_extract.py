import asyncio
from datetime import datetime
import logging
import pytest
import os
from pathlib import Path
from zipfile import ZipFile
from aggregator import extract, helper  # noqa

filename_example = "GBLogs_psc-n11_fanapiservice_1657563227839.zip"

sourcedir_example = [
    "GBLogs_-n11_fanapiservice_1657563227839.zip",
    'GBLogs_-n16_fanapiservice_1657563218539.zip',
]

module_name = "aggregator.extract"


@pytest.mark.mock
@pytest.mark.unit
def test_create_log_dir(logger, tmpdir):
    # Given a (viable) log directory (tmpdir)

    # When it tries to create that directory
    extract.create_log_dir(tmpdir)

    # Then it succeeds
    assert os.path.exists(tmpdir)
    # And it is a directory
    assert os.path.isdir(tmpdir)
    # And the logger logs success
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         f"Created {tmpdir}")
    ]


class MockPath:
    # Mock Path for FileNotFoundError & FileExistsError

    @staticmethod
    def mkdir_fnf():
        raise FileNotFoundError

    @staticmethod
    def mkdir_fee():
        raise FileExistsError


@pytest.mark.mutmut
@pytest.mark.mock
@pytest.mark.unit
def test_create_log_dir_parents_false(logger, tmpdir, monkeypatch):
    # Given a log dir as a subdirectory without a parent
    def mock_mkdir_fnf(*args, **kwargs):
        return MockPath.mkdir_fnf()

    monkeypatch.setattr(Path, "mkdir", mock_mkdir_fnf)

    # When it attempts to create the log dir
    # Then raises a FileNotFoundError
    with pytest.raises(FileNotFoundError):
        extract.create_log_dir(os.path.join(tmpdir, "no_parent", "sub"))

    # And the logger logs the error
    assert logger.record_tuples[0] == (
        module_name, logging.ERROR,
        "ErrorType: <class 'FileNotFoundError'> - Could not create directory"
    )


@pytest.mark.mutmut
@pytest.mark.mock
@pytest.mark.unit
def test_create_log_dir_exist_ok_false(logger, tmpdir, monkeypatch):
    # Given a log_directory file that already exists
    def mock_mkdir_fee(*args, **kwargs):
        return MockPath.mkdir_fee()

    monkeypatch.setattr(Path, "mkdir", mock_mkdir_fee)

    # When it attempts to create the directory
    # Then it raises a FileExistsError
    with pytest.raises(FileExistsError):
        extract.create_log_dir(tmpdir)

    # And the logger logs the error
    assert logger.record_tuples[0] == (
        module_name, logging.ERROR,
        "ErrorType: <class 'FileExistsError'> - Could not create directory"
    )


@ pytest.mark.unit
def test_move_files_to_target(logger, tmpdir):
    # Given a filename
    filename = "test.txt"
    # And a source directory
    file = tmpdir.mkdir("System").join(filename)
    # And a target path
    target = os.path.join(tmpdir, filename)
    # And content in the file
    file.write("text to test")
    # And a subdirectory
    sub = tmpdir.join("System")

    # When it moves files to target
    extract.move_files_to_target(tmpdir, "System")

    # Then the file will be in the source directory
    assert filename in os.listdir(tmpdir)
    # And the content of the file will remain as above
    with open(target, "r") as f:
        assert f.read() == "text to test"
    # And the file will not be in the sub directory
    assert filename not in os.listdir(sub)
    # And the logger will log it
    assert logger.record_tuples[0] == (
        module_name, logging.DEBUG,
        f"Moved {filename} from {sub} to {tmpdir}")


@ pytest.mark.unit
def test_remove_folder(logger, tmpdir):
    # Given a folder (tmpdir)

    # When it tries to remove the folder
    extract.remove_folder(tmpdir)

    # Then the folder no longer exists
    assert os.path.exists(tmpdir) is False

    # And the logger logs the removal
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         f"Removed {tmpdir}")
    ]


class MockZip:
    # Mock for Zip to return test namelist
    @ staticmethod
    def namelist():
        return [
            'System/fanapiservice.log.1',
            'System/fanapiservice.log',
        ]


@pytest.mark.mock
@pytest.mark.asyncio
@pytest.mark.unit
async def test_extract(logger, tmpdir, monkeypatch):
    # Given a test namelist
    def mock_zip_namelist(*args, **kwargs):
        return MockZip.namelist()

    monkeypatch.setattr(ZipFile, "namelist", mock_zip_namelist)

    # And an example filename
    zip_file = filename_example
    # And an example extension
    extension = "service.log"
    # And an empty log_filename
    log_file = ""

    # When it iterates through the Zip
    for filename in MockZip.namelist():
        if filename.endswith(extension):
            log_file = filename
    # And it tries to extract a file
    await extract.extract(
        zip_file, tmpdir, extension)

    # Then it extracts the file
    target_file = os.path.join(tmpdir, os.path.basename(log_file))
    assert os.path.exists(target_file)
    # And the logger logs the start of the coroutine
    logs = logger.record_tuples
    assert logs[0] == (
        module_name, logging.INFO,
        f"Starting extraction coroutine for {zip_file}")
    # And the logger logs the extraction of the file
    assert logs[1] == (
        module_name, logging.INFO,
        f"Extracted *{extension} generating {log_file} at {tmpdir}"
    )
    # And the logger logs the end of the coroutine
    assert logs[-1] == (
        module_name, logging.INFO,
        f"Ending extraction coroutine for {zip_file}"
    )


class MockNone:
    # Mock that returns None for testing

    @staticmethod
    def get_none(*args, **kwargs):
        return None


class MockDir:
    # Mock that returns an example directory output

    @staticmethod
    def listdir(dir: os.path):
        return sourcedir_example


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_extract_gen_extract_fn_list_empty(logger, monkeypatch, tmpdir):
    # Given an example source directory with zip files
    def mock_listdir(*args, **kwargs):
        return MockDir.listdir(tmpdir)

    monkeypatch.setattr(os, "listdir", mock_listdir)

    # When it tries to extract files without a list of functions
    # Then it raises an AttributeError
    with pytest.raises(AttributeError):
        await extract.gen_zip_extract_fn_list(tmpdir, None)
    # And the logger logs an AttributeError
    assert logger.record_tuples[-1][2].startswith(
        "Attribute Error:"
    )


@pytest.mark.parametrize(
    "get_node, get_log_type, get_log_dir",
    [
        (
            MockNone.get_none(),
            helper.get_log_type(filename_example),
            helper.get_log_dir("node", "fanapiservice"),
        ),
        (
            helper.get_node(filename_example),
            MockNone.get_none(),
            helper.get_log_dir("node", "fanapiservice"),

        ),
        (
            helper.get_node(filename_example),
            helper.get_log_type(filename_example),
            MockNone.get_none()
        ),
    ]
)
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.asyncio
@pytest.mark.unit
async def test_gen_extract_fn_list_helper_none_returns(
        logger, tmpdir, monkeypatch, get_node, get_log_type, get_log_dir):
    # Given a source directory
    def mock_listdir(*args, **kwargs):
        return MockDir.listdir(tmpdir)

    monkeypatch.setattr(os, "listdir", mock_listdir)

    # And a node
    def mock_helper_get_node(*args, **kwargs):
        return get_node

    monkeypatch.setattr(helper, "get_node", mock_helper_get_node)

    # And a log_type
    def mock_helper_get_log_type(*args, **kwargs):
        return get_log_type

    monkeypatch.setattr(helper, "get_log_type", mock_helper_get_log_type)

    # And a log_dir returned

    def mock_helper_get_log_dir(*args, **kwargs):
        return get_log_dir

    monkeypatch.setattr(helper, "get_log_dir", mock_helper_get_log_dir)

    # When it tries to extract the zip function list
    # Then it raises a TypeError
    with pytest.raises(TypeError):
        await extract.gen_zip_extract_fn_list(tmpdir, None)

    # And the logger logs it
    assert logger.record_tuples[-1][0] == module_name
    assert logger.record_tuples[-1][1] == logging.ERROR
    assert logger.record_tuples[-1][2] == "TypeError: Value should not be None"


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_gen_extract_fn_list_None_list(logger, tmpdir, monkeypatch):
    # Given a source directory
    def mock_listdir(*args, **kwargs):
        return MockDir.listdir(tmpdir)

    monkeypatch.setattr(os, "listdir", mock_listdir)

    # Given a test namelist
    def mock_zip_namelist(*args, **kwargs):
        return MockZip.namelist()

    monkeypatch.setattr(ZipFile, "namelist", mock_zip_namelist)

    # And a mock gather function
    def mock_asyncio_gather_get_none(*args, **kwargs):
        return MockNone.get_none(*args)

    monkeypatch.setattr(asyncio, "gather", mock_asyncio_gather_get_none)

    # When it tries to extract the zip function list
    # Then it raises a TypeError
    with pytest.raises(AttributeError):
        await extract.gen_zip_extract_fn_list(tmpdir, None)

    # And the logger logs it
    assert logger.record_tuples[-1] == (
        module_name, logging.ERROR,
        "Attribute Error: 'NoneType' object has no attribute 'append'"
    )


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_extract_log_asyncio_returns_none(logger, monkeypatch):
    # Given a mock asyncio gather gunction
    def mock_asyncio_gather_get_none(*args, **kwargs):
        return MockNone.get_none(*args)

    monkeypatch.setattr(asyncio, "gather", mock_asyncio_gather_get_none)

    # When it tries to extract the log
    # Then it raises an error
    with pytest.raises(TypeError):
        await extract.extract_log()

    # And the logger logs it
    assert logger.record_tuples[-1] == (
        module_name, logging.ERROR,
        "ErrorType: <class 'TypeError'> - asyncio gather failed"
    )


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_extract_log_asyncio_returns_FnF(logger, tmpdir):
    # Given a mock zip_file_extract_fn_list
    extract_fn_list = []
    extract_fn_list.append(
        extract.extract('./notafile.zip', tmpdir)
    )

    # When it tries to extract the log
    # Then it raises an error
    with pytest.raises(FileNotFoundError):
        await extract.extract_log(extract_fn_list)

    # And the logger logs it
    assert logger.record_tuples[-1] == (
        module_name, logging.ERROR,
        "ErrorType: <class 'FileNotFoundError'> - asyncio gather failed"
    )
