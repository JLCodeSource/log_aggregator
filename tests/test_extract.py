import logging
import pytest
import os
import extract


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
