import asyncio
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
def test_create_log_dir(logger, tmpdir):
    extract.create_log_dir(tmpdir)
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         f"Created {tmpdir}")
    ]


class MockPath:

    @staticmethod
    def mkdir_fnf():
        raise FileNotFoundError

    @staticmethod
    def mkdir_fee():
        raise FileExistsError


@pytest.mark.mutmut
@pytest.mark.mock
def test_create_log_dir_parents_false(logger, tmpdir, monkeypatch):
    def mock_mkdir_fnf(*args, **kwargs):
        return MockPath.mkdir_fnf()

    monkeypatch.setattr(Path, "mkdir", mock_mkdir_fnf)

    with pytest.raises(FileNotFoundError):
        extract.create_log_dir(os.path.join(tmpdir, "no_parent", "sub"))

    assert logger.record_tuples[0][0] == module_name
    assert logger.record_tuples[0][1] == logging.ERROR
    assert logger.record_tuples[0][2] == (
        "ErrorType: <class 'FileNotFoundError'> - Could not create directory"
    )


@pytest.mark.mutmut
@pytest.mark.mock
def test_create_log_dir_exist_ok_false(logger, tmpdir, monkeypatch):
    def mock_mkdir_fee(*args, **kwargs):
        return MockPath.mkdir_fee()

    monkeypatch.setattr(Path, "mkdir", mock_mkdir_fee)

    with pytest.raises(FileExistsError):
        extract.create_log_dir(tmpdir)

    assert logger.record_tuples[0][0] == module_name
    assert logger.record_tuples[0][1] == logging.ERROR
    assert logger.record_tuples[0][2] == (
        "ErrorType: <class 'FileExistsError'> - Could not create directory"
    )


@ pytest.mark.unit
def test_move_files_to_target(logger, tmpdir):
    filename = "test.txt"
    file = tmpdir.mkdir("System").join(filename)
    sub = tmpdir.join("System")
    file.write("text to test.txt\n")
    extract.move_files_to_target(tmpdir, "System")
    assert filename in os.listdir(tmpdir)
    assert filename not in os.listdir(sub)
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         f"Moved {filename} from {sub} to {tmpdir}")
    ]


@ pytest.mark.unit
def test_remove_folder(logger, tmpdir):
    extract.remove_folder(tmpdir)
    assert os.path.exists(tmpdir) is False
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         f"Removed {tmpdir}")
    ]


class MockZip:

    @ staticmethod
    def namelist():
        return [
            'System/fanapiservice.log.1',
            'System/fanapiservice.log',
        ]


@ pytest.mark.mock
@ pytest.mark.asyncio
async def test_extract(logger, tmpdir, monkeypatch):

    def mock_zip_namelist(*args, **kwargs):
        return MockZip.namelist()

    monkeypatch.setattr(ZipFile, "namelist", mock_zip_namelist)

    file = filename_example
    extension = "service.log"
    log_file = ""
    for filename in MockZip.namelist():
        if filename.endswith(extension):
            log_file = filename
    await extract.extract(file,
                          tmpdir, extension)
    logs = logger.record_tuples
    assert logs[0] == (
        module_name,
        logging.INFO,
        f"Starting extraction coroutine for {file}")
    assert logs[1] == (
        module_name,
        logging.INFO,
        f"Extracted *{extension} generating {log_file} at {tmpdir}"
    )


class MockNone:

    @staticmethod
    def get_none(file: str, log_type: str | None = None):
        return None


class MockDir:

    @staticmethod
    def listdir(dir: os.path):
        return sourcedir_example


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
async def test_extract_gen_extract_fn_list_empty(logger, monkeypatch, tmpdir):
    def mock_listdir(*args, **kwargs):
        return MockDir.listdir(tmpdir)

    monkeypatch.setattr(os, "listdir", mock_listdir)

    with pytest.raises(AttributeError):
        await extract.gen_zip_extract_fn_list(tmpdir, None)

    assert logger.record_tuples[-1][2].startswith(
        "Attribute Error:"
    )


@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.asyncio
async def test_extract_log_helper_node_none(logger, tmpdir, monkeypatch):
    def mock_listdir(*args, **kwargs):
        return MockDir.listdir(tmpdir)

    def mock_helper_get_node(file):
        return MockNone.get_none(file)

    def mock_asyncio_gather_get_none(*args, **kwargs):
        return MockNone.get_none(*args)

    monkeypatch.setattr(os, "listdir", mock_listdir)

    monkeypatch.setattr(helper, "get_node", mock_helper_get_node)

    monkeypatch.setattr(asyncio, "gather", mock_asyncio_gather_get_none)

    with pytest.raises(Exception):
        log = await extract.extract_log(tmpdir, None, None)
        assert log[-1][2].startswith("Fail:")
