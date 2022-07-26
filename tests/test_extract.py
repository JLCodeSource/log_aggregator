import logging
import pytest
import os
import extract


filename_data = [
    ("newnode", "newservice", True),
    ("newnode", "newservice", False),
    ("Complex1£", "Svc23e&", True),
    ("Complex1£", "Svc23e&", False),
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
def test_get_node(logger, make_filename, node, service, tld):
    file = make_filename(node, service, tld)
    assert extract.get_node(file) == node
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"node: {node} from {file}")
    ]


@pytest.mark.unit
def test_get_log_dir(logger, settings_override):
    node = "node"
    log_type = "fanapiservice"
    out = os.path.join(settings_override.outdir, node, log_type)
    test = extract.get_log_dir("node", "fanapiservice")
    assert test == out
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"outdir: {out} from {settings_override.outdir}, {node}, {log_type}")]
