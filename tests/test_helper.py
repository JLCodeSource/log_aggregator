import logging
import pytest
import os
import helper
from config import get_settings

settings = get_settings()

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
        ("helper", logging.DEBUG, f"node: {node} from {file}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_log_type(logger, make_filename, node, service, ext, tld):
    file = make_filename(node, service, ext, tld)
    assert helper.get_log_type(file) == service
    assert logger.record_tuples == [
        ("helper", logging.DEBUG,
         f"log_type: {service} from {file}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_log_dir(logger, settings_override, node, service, ext, tld):
    out = os.path.join(settings_override.outdir, node, service)
    test = helper.get_log_dir(node, service)
    assert test == out
    assert logger.record_tuples == [
        ("helper", logging.DEBUG,
         f"outdir: {out} from {settings_override.outdir}, {node}, {service}")]
