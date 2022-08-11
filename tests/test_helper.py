import logging
import pytest
import os
from aggregator import config, helper


settings = config.get_settings()

module_name = "aggregator.helper"

filename_data = [
    ("newnode", "newservice", ".zip", True),
    ("newnode", "newservice", ".zip", False),
    ("Complex-1", "Svc-23e", ".zip", True),
    ("Complex-1", "Svc-23e", ".zip", False),
    ("newnode", "newservice", ".log", True),
    ("newnode", "newservice", ".log", False),
    ("Complex-1", "Svc-23e", ".log", True),
    ("Complex-1", "Svc-23e", ".log", False),
]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_node(logger, make_filename, node, service, ext, tld):
    file = make_filename(node, service, ext, tld)
    assert helper.get_node(file) == node
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"node: {node} from {file}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_node_tmpdir(
        logger, make_filename, node, service, ext, tld, tmpdir):
    file = make_filename(node, service, ext, tld)
    if os.path.splitext(file)[1] == ".log":
        filename = f"{tmpdir}{file[1:]}"
    else:
        filename = f"{tmpdir}{os.path.sep}{file}"
    assert helper.get_node(filename) == node
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"node: {node} from {filename}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_log_type(logger, make_filename, node, service, ext, tld):
    file = make_filename(node, service, ext, tld)
    assert helper.get_log_type(file) == service
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         f"log_type: {service} from {file}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_log_type_tmpdir(
        logger, make_filename, node, service, ext, tld, tmpdir):
    file = make_filename(node, service, ext, tld)
    if os.path.splitext(file)[1] == ".log":
        filename = f"{tmpdir}{file[1:]}"
    else:
        filename = f"{tmpdir}{os.path.sep}{file}"
    assert helper.get_log_type(filename) == service
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         f"log_type: {service} from {filename}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_log_dir(logger, settings_override, node, service, ext, tld):
    out = os.path.join(settings_override.outdir, node, service)
    test = helper.get_log_dir(node, service)
    assert test == out
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         f"outdir: {out} from {settings_override.outdir}, {node}, {service}")]
