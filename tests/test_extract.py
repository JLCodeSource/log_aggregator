from cmath import log
import logging
from re import L
import pytest
import os
import extract
from zipfile import ZipFile

filename_data = [
    ("newnode", "newservice", True),
    ("newnode", "newservice", False),
    ("Complex-1", "Svc-23e", True),
    ("Complex-1", "Svc-23e", False),
]


@pytest.mark.parametrize("node, service, tld", filename_data)
@pytest.mark.unit
def test_get_node(logger, make_filename, node, service, tld):
    file = make_filename(node, service, tld)
    assert extract.get_node(file) == node
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"node: {node} from {file}")
    ]


@pytest.mark.parametrize("node, service, tld", filename_data)
@pytest.mark.unit
def test_get_log_type(logger, make_filename, node, service, tld):
    file = make_filename(node, service, tld)
    assert extract.get_log_type(file) == service
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"log_type: {service} from {file}")
    ]


@pytest.mark.parametrize("node, service, tld", filename_data)
@pytest.mark.unit
def test_get_log_dir(logger, settings_override, node, service, tld):
    out = os.path.join(settings_override.outdir, node, service)
    test = extract.get_log_dir(node, service)
    assert test == out
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"outdir: {out} from {settings_override.outdir}, {node}, {service}")]


@pytest.mark.unit
def test_create_log_dir(logger, tmpdir):
    extract.create_log_dir(tmpdir)
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"Created {tmpdir}")
    ]


@pytest.mark.unit
def test_move_files_to_target(logger, tmpdir):
    filename = "test.txt"
    file = tmpdir.mkdir("System").join(filename)
    sub = tmpdir.join("System")
    file.write("text to test.txt\n")
    extract.move_files_to_target(tmpdir, "System")
    assert filename in os.listdir(tmpdir)
    assert filename not in os.listdir(sub)
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"Moved {filename} from {sub} to {tmpdir}")
    ]


@pytest.mark.unit
def test_remove_folder(logger, tmpdir):
    extract.remove_folder(tmpdir)
    assert os.path.exists(tmpdir) is False
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"Removed {tmpdir}")
    ]


class MockZip:

    @staticmethod
    def namelist():
        return [
            'System/fanapiservice.log.1',
            'System/fanapiservice.log',
        ]


@pytest.mark.mock
def test_extract(logger, tmpdir, monkeypatch):

    def mock_zip_namelist(*args, **kwargs):
        return MockZip.namelist()

    monkeypatch.setattr(ZipFile, "namelist", mock_zip_namelist)

    file = "GBLogs_psc-n11_fanapiservice_1657563227839.zip"
    extension = "service.log"
    log_file = ""
    for filename in MockZip.namelist():
        if filename.endswith(extension):
            log_file = filename
    extract.extract(file,
                    tmpdir, extension)
    logs = logger.record_tuples
    assert logs[0] == ("extract", logging.INFO,
                       f"Extracted *{extension} generating "
                       + f"{log_file} at {tmpdir}")
