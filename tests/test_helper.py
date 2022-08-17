import logging
import os
from pathlib import Path
from re import Pattern
from typing import Callable

import pytest
from hypothesis import given
from hypothesis import strategies as st

from aggregator import config, helper
from aggregator.helper import LOG_LOGTYPE_PATTERN  # noqa
from aggregator.helper import LOG_NODE_PATTERN  # noqa
from aggregator.helper import ZIP_LOGTYPE_PATTERN  # noqa
from aggregator.helper import ZIP_NODE_PATTERN  # noqa; noqa

PATTERNS: list[Pattern[str]] = [
    ZIP_LOGTYPE_PATTERN,
    ZIP_NODE_PATTERN,
    LOG_LOGTYPE_PATTERN,
    LOG_NODE_PATTERN,
]

settings: config.Settings = config.get_settings()

module_name: str = "aggregator.helper"


filename_node_data: list[tuple[str, str, str, bool, Pattern]] = [
    ("newnode", "newservice", ".zip", True, ZIP_NODE_PATTERN),
    ("newnode", "newservice", ".zip", False, ZIP_NODE_PATTERN),
    ("Complex-1", "Svc-23e", ".zip", True, ZIP_NODE_PATTERN),
    ("Complex-1", "Svc-23e", ".zip", False, ZIP_NODE_PATTERN),
    ("newnode", "newservice", ".log", True, LOG_NODE_PATTERN),
    ("newnode", "newservice", ".log", False, LOG_NODE_PATTERN),
    ("Complex-1", "Svc-23e", ".log", True, LOG_NODE_PATTERN),
    ("Complex-1", "Svc-23e", ".log", False, LOG_NODE_PATTERN),
]

filename_logtype_data: list[tuple[str, str, str, bool, Pattern]] = [
    ("newnode", "newservice", ".zip", True, ZIP_LOGTYPE_PATTERN),
    ("newnode", "newservice", ".zip", False, ZIP_LOGTYPE_PATTERN),
    ("Complex-1", "Svc-23e", ".zip", True, ZIP_LOGTYPE_PATTERN),
    ("Complex-1", "Svc-23e", ".zip", False, ZIP_LOGTYPE_PATTERN),
    ("newnode", "newservice", ".log", True, LOG_LOGTYPE_PATTERN),
    ("newnode", "newservice", ".log", False, LOG_LOGTYPE_PATTERN),
    ("Complex-1", "Svc-23e", ".log", True, LOG_LOGTYPE_PATTERN),
    ("Complex-1", "Svc-23e", ".log", False, LOG_LOGTYPE_PATTERN),
]


@pytest.mark.parametrize("node, service, ext, tld, pattern", filename_node_data)
@pytest.mark.unit
def test_get_node(
    logger: pytest.LogCaptureFixture,
    make_filename: Callable[[str, str, str, bool], Path],
    node: str,
    service: str,
    ext: str,
    tld: bool,
    pattern: Pattern,
) -> None:
    file: Path = make_filename(node, service, ext, tld)
    assert helper.get_node(file, pattern) == node
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"node: {node} from {file}")
    ]


@pytest.mark.parametrize("node, service, ext, tld, pattern", filename_node_data)
@pytest.mark.unit
def test_get_node_tmp_path(
    logger: pytest.LogCaptureFixture,
    make_filename: Callable[[str, str, str, bool], Path],
    node: str,
    service: str,
    ext: str,
    tld: bool,
    pattern: Pattern,
    tmp_path: Path,
) -> None:
    file: Path = make_filename(node, service, ext, tld)
    if ext == ".log":
        filename: Path = Path(os.path.abspath(file))
        filename = Path(os.path.join(tmp_path, filename))
    else:
        filename = Path(os.path.join(tmp_path, file))
    assert helper.get_node(filename, pattern) == node
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"node: {node} from {filename}")
    ]


@pytest.mark.parametrize("node, service, ext, tld, pattern", filename_logtype_data)
@pytest.mark.unit
def test_get_log_type(
    logger: pytest.LogCaptureFixture,
    make_filename: Callable[[str, str, str, bool], Path],
    node: str,
    service: str,
    ext: str,
    tld: bool,
    pattern: Pattern,
) -> None:
    file: Path = make_filename(node, service, ext, tld)
    assert helper.get_log_type(file, pattern) == service
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"log_type: {service} from {file}")
    ]


@pytest.mark.parametrize("node, service, ext, tld, pattern", filename_logtype_data)
@pytest.mark.unit
def test_get_log_type_tmpdir(
    logger: pytest.LogCaptureFixture,
    make_filename: Callable[[str, str, str, bool], Path],
    node: str,
    service: str,
    ext: str,
    tld: bool,
    pattern: Pattern,
    tmp_path: Path,
) -> None:
    file: Path = make_filename(node, service, ext, tld)
    if ext == ".log":
        filename: Path = Path(os.path.abspath(file))
        filename = Path(os.path.join(tmp_path, filename))
    else:
        filename = Path(tmp_path, file)
    assert helper.get_log_type(filename, pattern) == service
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"log_type: {service} from {filename}")
    ]


@pytest.mark.parametrize("node, service, ext, tld, pattern", filename_node_data)
@pytest.mark.unit
def test_get_log_dir(
    logger: pytest.LogCaptureFixture,
    settings_override: config.Settings,
    node: str,
    service: str,
    ext: str,
    tld: bool,
    pattern: Pattern,
) -> None:
    out: Path = Path(os.path.join(settings_override.outdir, node, service))
    test: Path = helper.get_log_dir(node, service)
    assert test == out
    assert logger.record_tuples == [
        (
            module_name,
            logging.DEBUG,
            f"outdir: {out} from {settings_override.outdir}, {node}, {service}",
        )
    ]


class TestWithInferredStrategies:
    """Test all functions from helper with inferred Hypothesis strategies."""

    def test_get_log_dir(self) -> None:
        @given(node=st.text(), log_type=st.text())
        def execute(**kwargs) -> None:
            helper.get_log_dir(**kwargs)

        execute()
