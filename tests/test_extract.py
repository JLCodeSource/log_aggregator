import logging
import pytest
import os
from pathlib import Path
from zipfile import ZipFile
from aggregator import extract  # noqa

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
    def mkdir():
        raise FileNotFoundError


@pytest.mark.mock
def test_create_log_dir_parents_false(logger, tmpdir, monkeypatch):
    def mock_mkdir(*args, **kwargs):
        return MockPath.mkdir()

    monkeypatch.setattr(Path, "mkdir", mock_mkdir)

    with pytest.raises(FileNotFoundError):
        extract.create_log_dir(os.path.join(tmpdir, "no_parent", "sub"))

    assert logger.record_tuples[0][0] == module_name
    assert logger.record_tuples[0][1] == logging.ERROR
    assert logger.record_tuples[0][2].startswith(
        "Could not create directory:"
    )


class MockDir:

    @staticmethod
    def listdir(dir: os.path):
        return sourcedir_example


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
async def test_extract_log_empty_fn_list(logger, monkeypatch, tmpdir):
    def mock_listdir(*args, **kwargs):
        return MockDir.listdir(tmpdir)

    monkeypatch.setattr(os, "listdir", mock_listdir)

    with pytest.raises(AttributeError):
        await extract.extract_log(tmpdir, None)
    print(logger.record_tuples[:-1])
    assert logger.record_tuples[-1][2].startswith(
        "Attribute Error:"
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

"""
@pytest.mark.mock
@pytest.mark.asyncio
async def test_extract_log(logger, tmpdir, monkeypatch, one_line_log,
                           settings_override):

    def mock_listdir(tmpdir):
        return sourcedir_example

    monkeypatch.setattr(os, "listdir", mock_listdir)

    def mock_convert():
        return one_line_log()

    monkeypatch.setattr(extract.extract_log, "convert", mock_convert,
                        raising=False)

    # log_len = len(one_line_log.splitlines())

    # settings = settings_override

    await extract.extract_log(tmpdir)

    # logs = logger.record_tuples
    # assert logs[0] == (
    #    module_name, logging.INFO,
    #    f"Inserted {log_len} into {settings.database}") """
