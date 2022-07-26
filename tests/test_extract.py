import logging
import pytest
import os
import extract


@pytest.mark.unit
def test_get_log_dir(logger, settings):
    node = "node"
    log_type = "fanapiservice"
    out = os.path.join(settings.outdir, node, log_type)
    test = extract.get_log_dir("node", "fanapiservice")
    assert test == out
    assert logger.record_tuples == [
        ("extract", logging.DEBUG,
         f"outdir: {out} from {settings.outdir}, {node}, {log_type}")]
