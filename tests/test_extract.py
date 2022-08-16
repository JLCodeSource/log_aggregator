import asyncio
import inspect
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Coroutine, NoReturn
from zipfile import BadZipFile, ZipFile

import pytest

from aggregator import config, extract, helper  # noqa

filename_example: str = "GBLogs_-n11_fanapiservice_1657563227839.zip"
badzipfile_example: str = "not_a_zip.zip"
non_file: str = "non_file.zip"

sourcedir_example: list[str] = [
    "GBLogs_-n11_fanapiservice_1657563227839.zip",
    "GBLogs_-n16_fanapiservice_1657563218539.zip",
]

module_name: str = "aggregator.extract"


@pytest.mark.mock
@pytest.mark.unit
def test_create_log_dir(
    logger: pytest.LogCaptureFixture, tmpdir: pytest.TempdirFactory
) -> None:
    # Given a (viable) log directory (tmpdir)

    # When it tries to create that directory
    extract._create_log_dir(str(tmpdir))

    # Then it succeeds
    assert os.path.exists(str(tmpdir))
    # And it is a directory
    assert os.path.isdir(str(tmpdir))
    # And the logger logs success
    assert logger.record_tuples == [(module_name, logging.DEBUG, f"Created {tmpdir}")]


class MockPath:
    # Mock Path for FileNotFoundError & FileExistsError

    @staticmethod
    def mkdir_fnf() -> NoReturn:
        raise FileNotFoundError

    @staticmethod
    def mkdir_fee() -> NoReturn:
        raise FileExistsError


@pytest.mark.mutmut
@pytest.mark.mock
@pytest.mark.unit
def test_create_log_dir_parents_false(
    logger: pytest.LogCaptureFixture,
    tmpdir: pytest.TempdirFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a log dir as a subdirectory without a parent
    def mock_mkdir_fnf(*args, **kwargs) -> NoReturn:
        raise MockPath.mkdir_fnf()

    monkeypatch.setattr(Path, "mkdir", mock_mkdir_fnf)

    # When it attempts to create the log dir
    # Then raises a FileNotFoundError
    with pytest.raises(FileNotFoundError):
        extract._create_log_dir(os.path.join(str(tmpdir), "no_parent", "sub"))

    # And the logger logs the error
    assert logger.record_tuples[0] == (
        module_name,
        logging.ERROR,
        "ErrorType: <class 'FileNotFoundError'> - Could not create directory",
    )


@pytest.mark.mutmut
@pytest.mark.mock
@pytest.mark.unit
def test_create_log_dir_exist_ok_false(
    logger: pytest.LogCaptureFixture,
    tmpdir: pytest.TempdirFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a log_directory file that already exists
    def mock_mkdir_fee(*args, **kwargs) -> NoReturn:
        raise MockPath.mkdir_fee()

    monkeypatch.setattr(Path, "mkdir", mock_mkdir_fee)

    # When it attempts to create the directory
    # Then it raises a FileExistsError
    with pytest.raises(FileExistsError):
        extract._create_log_dir(str(tmpdir))

    # And the logger logs the error
    assert logger.record_tuples[0] == (
        module_name,
        logging.ERROR,
        "ErrorType: <class 'FileExistsError'> - Could not create directory",
    )


@pytest.mark.unit
def test_move_files_to_target(
    logger: pytest.LogCaptureFixture, tmpdir: pytest.TempdirFactory
) -> None:
    # Given a filename
    filename: str = "test.txt"
    # And a source directory
    system: str = os.path.join(str(tmpdir), "System")
    os.mkdir(system)
    file: str = os.path.join(system, filename)
    # And a target path
    target: str = os.path.join(str(tmpdir), filename)
    # And content in the file
    with open(file, "w") as f:
        f.write("text to test")

    # When it moves files to target
    extract._move_files_to_target(str(tmpdir), system)

    # Then the file will be in the source directory
    assert filename in os.listdir(str(tmpdir))
    # And the content of the file will remain as above
    with open(target, "r") as f:
        assert f.read() == "text to test"
    # And the file will not be in the sub directory
    assert filename not in os.listdir(system)
    # And the logger will log it
    assert logger.record_tuples[0] == (
        module_name,
        logging.DEBUG,
        f"Moved {filename} from {system} to {tmpdir}",
    )


@pytest.mark.unit
def test_remove_folder(
    logger: pytest.LogCaptureFixture, tmpdir: pytest.TempdirFactory
) -> None:
    # Given a folder (tmpdir)

    # When it tries to remove the folder
    extract._remove_folder(str(tmpdir))

    # Then the folder no longer exists
    assert os.path.exists(str(tmpdir)) is False

    # And the logger logs the removal
    assert logger.record_tuples == [(module_name, logging.DEBUG, f"Removed {tmpdir}")]


@pytest.mark.unit
def test_remove_folder_fnf(
    logger: pytest.LogCaptureFixture, tmpdir: pytest.TempdirFactory
) -> None:
    # Given a non-existent temp dir
    os.rmdir(str(tmpdir))

    # When it tries to remove the folder
    # Then it raises a FileNotFoundError
    with pytest.raises(FileNotFoundError):
        extract._remove_folder(str(tmpdir))

    # And the logger logs it
    assert logger.record_tuples[0][0] == module_name
    assert logger.record_tuples[0][1] == logging.ERROR
    assert logger.record_tuples[0][2].startswith("FileNotFoundError:")


class MockZip:
    # Mock for Zip to return test namelist
    @staticmethod
    def namelist() -> list[str]:
        return [
            "System/fanapiservice.log.1",
            "System/fanapiservice.log",
        ]


@pytest.mark.mock
@pytest.mark.asyncio
@pytest.mark.integration
async def test_extract_successful_run(
    logger: pytest.LogCaptureFixture,
    tmpdir: pytest.TempdirFactory,
    settings_override: config.Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a test namelist
    def mock_zip_namelist(*args, **kwargs) -> list[str]:
        return MockZip.namelist()

    monkeypatch.setattr(ZipFile, "namelist", mock_zip_namelist)

    # And an example filename
    zip_file: str = filename_example
    src_dir: str = settings_override.get_sourcedir()
    src_file: str = os.path.join(src_dir, zip_file)
    tgt_zip: str = os.path.join(str(tmpdir), zip_file)
    shutil.copy(src_file, str(tmpdir))
    # And an example extension
    extension: str = "service.log"
    # And an empty log_filename
    log_file: str = ""

    # When it iterates through the Zip
    for filename in MockZip.namelist():
        if filename.endswith(extension):
            log_file = filename
    # And it tries to extract a file
    await extract._extract(tgt_zip, str(tmpdir), extension)

    # Then it extracts the file
    target_log: str = os.path.join(str(tmpdir), os.path.basename(log_file))
    assert os.path.exists(target_log)
    # And the logger logs the start of the coroutine
    logs: list[tuple[str, int, str]] = logger.record_tuples
    assert logs[0] == (
        module_name,
        logging.INFO,
        f"Starting extraction coroutine for {tgt_zip}",
    )
    # And the logger logs the extraction of the file
    assert logs[1] == (
        module_name,
        logging.INFO,
        f"Extracted *{extension} generating {log_file} at {tmpdir}",
    )
    # And the logger logs the end of the coroutine
    assert logs[-1] == (
        module_name,
        logging.INFO,
        f"Ending extraction coroutine for {tgt_zip}",
    )


@pytest.mark.mock
@pytest.mark.asyncio
@pytest.mark.integration
async def test_extract_badzipfile(
    logger: pytest.LogCaptureFixture,
    tmpdir: pytest.TempdirFactory,
    settings_override: config.Settings,
) -> None:
    # And an example filename
    zip_file: str = badzipfile_example
    src_dir: str = settings_override.get_testdatadir()
    src_file: str = os.path.join(src_dir, zip_file)
    tgt_file: str = os.path.join(str(tmpdir), zip_file)
    shutil.copy(src_file, str(tmpdir))

    # When it tries to extract the zip
    # Then it raises
    with pytest.raises(BadZipFile):
        await extract._extract(tgt_file, str(tmpdir))

    # And it logs the error
    assert logger.record_tuples[-1] == (
        module_name,
        logging.WARN,
        f"BadZipFile: {tgt_file} is a BadZipFile",
    )


class MockNone:
    # Mock that returns None for testing

    @staticmethod
    def get_none(*args, **kwargs) -> None:
        return None


class MockEmpty:
    # Mock that returns empty for testing

    @staticmethod
    def get_empty(*args, **kwargs) -> list:
        lst = []  # type: ignore
        return lst


class MockDir:
    # Mock that returns an example directory output

    @staticmethod
    def listdir(dir: str) -> list[str]:
        return sourcedir_example


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.unit
async def test_gen_extract_fn_list(
    monkeypatch: pytest.MonkeyPatch, tmpdir: pytest.TempdirFactory
) -> None:
    # Given a mock example source directory
    def mock_listdir(*args, **kwargs) -> list[str]:
        return MockDir.listdir(str(tmpdir))

    monkeypatch.setattr(os, "listdir", mock_listdir)

    # When it tries to generate the extract files list
    zip_files_extract_fn_list: list[
        Coroutine[Any, Any, list[str]]
    ] | None = extract.gen_zip_extract_fn_list(str(tmpdir))

    # Then it returns a list of functions
    assert zip_files_extract_fn_list is not None
    assert inspect.iscoroutine(zip_files_extract_fn_list[0]) is True


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_gen_extract_fn_list_empty(
    logger: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmpdir: pytest.TempdirFactory,
) -> None:
    # Given an example source directory with zip files
    def mock_listdir(*args, **kwargs) -> list[str]:
        return MockDir.listdir(str(tmpdir))

    monkeypatch.setattr(os, "listdir", mock_listdir)

    # When it tries to extract files without a list of functions
    coro_list: list[Coroutine[Any, Any, list[str]]] | None = []
    # Then it raises an AttributeError
    with pytest.raises(FileNotFoundError):
        coro_list = extract.gen_zip_extract_fn_list(str(tmpdir), coro_list)
        assert coro_list is not None
        for coro in coro_list:
            await coro
    # And the logger logs an AttributeError
    assert logger.record_tuples[-1][2].startswith("FileNotFoundError:")


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
            MockNone.get_none(),
        ),
    ],
)
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.asyncio
@pytest.mark.unit
async def test_gen_extract_fn_list_helper_none_returns(
    logger: pytest.LogCaptureFixture,
    tmpdir: pytest.TempdirFactory,
    monkeypatch: pytest.MonkeyPatch,
    get_node: str,
    get_log_type: str,
    get_log_dir: str,
) -> None:
    # Given a source directory
    def mock_listdir(*args, **kwargs) -> list[str]:
        return MockDir.listdir(str(tmpdir))

    monkeypatch.setattr(os, "listdir", mock_listdir)

    # And a node
    def mock_helper_get_node(*args, **kwargs) -> str:
        return get_node

    monkeypatch.setattr(helper, "get_node", mock_helper_get_node)

    # And a log_type
    def mock_helper_get_log_type(*args, **kwargs) -> str:
        return get_log_type

    monkeypatch.setattr(helper, "get_log_type", mock_helper_get_log_type)

    # And a log_dir returned

    def mock_helper_get_log_dir(*args, **kwargs) -> str:
        return get_log_dir

    monkeypatch.setattr(helper, "get_log_dir", mock_helper_get_log_dir)

    # When it tries to extract the zip function list
    # Then it raises a TypeError
    with pytest.raises(TypeError):
        coro_list: list[
            Coroutine[Any, Any, list[str]]
        ] | None = extract.gen_zip_extract_fn_list(str(tmpdir), None)
        assert coro_list is not None
        for coro in coro_list:
            await coro

    # And the logger logs it
    assert logger.record_tuples[-1][0] == module_name
    assert logger.record_tuples[-1][1] == logging.ERROR
    assert logger.record_tuples[-1][2] == "TypeError: Value should not be None"


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_gen_extract_fn_list_None_list(
    logger: pytest.LogCaptureFixture,
    tmpdir: pytest.TempdirFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a source directory
    def mock_listdir(*args, **kwargs) -> list[str]:
        return MockDir.listdir(str(tmpdir))

    monkeypatch.setattr(os, "listdir", mock_listdir)

    # Given a test namelist
    def mock_zip_namelist(*args, **kwargs) -> list[str]:
        return MockZip.namelist()

    monkeypatch.setattr(ZipFile, "namelist", mock_zip_namelist)

    # And a mock gather function
    def mock_asyncio_gather_get_none(*args, **kwargs) -> None:
        return MockNone.get_none(*args)

    monkeypatch.setattr(asyncio, "gather", mock_asyncio_gather_get_none)

    # When it tries to extract the zip function list
    # Then it raises a TypeError
    with pytest.raises(AttributeError):
        extract.gen_zip_extract_fn_list(str(tmpdir), None)

    # And the logger logs it
    assert logger.record_tuples[-1] == (
        module_name,
        logging.ERROR,
        "Attribute Error: 'NoneType' object has no attribute 'append'",
    )


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.integration
async def test_extract_log_returns_log_files(
    logger: pytest.LogCaptureFixture,
    tmpdir: pytest.TempdirFactory,
    settings_override: config.Settings,
) -> None:
    # Given  a target directory
    src_dir: str = settings_override.get_sourcedir()
    # And a zip file
    file_full_path: str = os.path.join(str(tmpdir), filename_example)
    # And a log to be extracted (filename_example)
    src_file: str = os.path.join(src_dir, filename_example)
    shutil.copy(src_file, str(tmpdir))

    # And a list of coroutines
    coroutine_list: list = []
    coroutine_list.append(extract._extract(file_full_path, str(tmpdir)))

    # When it tries to extract the log list
    log_files: list[str] = []
    log_file_list: list[str] = await extract.extract_log(coroutine_list)
    for file in log_file_list:
        log_files.append(file)

    # The extracted file is in the log_files output
    assert file_full_path in log_files[0]

    # And the logger logs the extraction
    assert logger.record_tuples[1] == (
        module_name,
        logging.INFO,
        f"Extracted *service.log generating System/fanapiservice.log at {tmpdir}",
    )


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_extract_log_asyncio_returns_empty(
    logger: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a mock asyncio gather function
    def mock_asyncio_gather_get_empty(*args, **kwargs) -> list:
        return MockEmpty.get_empty(*args)

    monkeypatch.setattr(asyncio, "gather", mock_asyncio_gather_get_empty)

    # And a mock list
    lst: list[Coroutine[Any, Any, list[str]]] = []

    # When it tries to extract the log
    # Then it raises an error
    with pytest.raises(TypeError):
        await extract.extract_log(lst)

    # And the logger logs it
    assert logger.record_tuples[-1] == (
        module_name,
        logging.ERROR,
        "ErrorType: <class 'TypeError'> - asyncio gather failed",
    )


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_extract_log_asyncio_returns_FnF(
    logger: pytest.LogCaptureFixture, tmpdir: pytest.TempdirFactory
) -> None:
    # Given a non file
    file: str = os.path.join(str(tmpdir), non_file)
    # And a mock zip_file_extract_fn_list
    extract_fn_list: list[Coroutine[Any, Any, list[str]]] = []
    extract_fn_list.append(extract._extract(file, str(tmpdir)))

    # When it tries to extract the log
    # Then it raises an error
    with pytest.raises(FileNotFoundError):
        await extract.extract_log(extract_fn_list)

    # And the logger logs it
    assert logger.record_tuples[-1] == (
        module_name,
        logging.ERROR,
        "ErrorType: <class 'FileNotFoundError'> - asyncio gather failed",
    )


@pytest.mark.asyncio
@pytest.mark.mock
@pytest.mark.mutmut
@pytest.mark.unit
async def test_extract_log_asyncio_gather_type(
    logger: pytest.LogCaptureFixture, tmpdir: pytest.TempdirFactory
) -> None:
    # Given a non file
    file: str = os.path.join(str(tmpdir), non_file)
    # And a mock zip_file_extract_fn_list
    extract_fn_list: list[Coroutine[Any, Any, list[str]]] = []
    extract_fn_list.append(extract._extract(file, str(tmpdir)))

    # When it tries to extract the log
    # it raises a FileNotFound Error
    with pytest.raises(FileNotFoundError):
        await asyncio.gather(*extract_fn_list)

    # And the logger logs it
    assert logger.record_tuples[-1] == (
        module_name,
        logging.ERROR,
        f"FileNotFoundError: {file} is not a file",
    )
