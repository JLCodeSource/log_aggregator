import logging
import os
from typing import Callable

import pytest
from hypothesis import given, strategies as st

from aggregator import config, helper

settings: config.Settings = config.get_settings()

module_name: str = "aggregator.helper"

filename_data: list[tuple[str, str, str, bool]] = [
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
def test_get_node(
    logger: pytest.LogCaptureFixture,
    make_filename: Callable[[str, str, str, bool], str],
    node: str,
    service: str,
    ext: str,
    tld: bool,
) -> None:
    file: str = make_filename(node, service, ext, tld)
    assert helper.get_node(file) == node
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"node: {node} from {file}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_node_tmpdir(
    logger: pytest.LogCaptureFixture,
    make_filename: Callable[[str, str, str, bool], str],
    node: str,
    service: str,
    ext: str,
    tld: bool,
    tmpdir: pytest.TempdirFactory,
) -> None:
    file: str = make_filename(node, service, ext, tld)
    if os.path.splitext(file)[1] == ".log":
        filename: str = f"{tmpdir}{file[1:]}"
    else:
        filename = f"{tmpdir}{os.path.sep}{file}"
    assert helper.get_node(filename) == node
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"node: {node} from {filename}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_log_type(
    logger: pytest.LogCaptureFixture,
    make_filename: Callable[[str, str, str, bool], str],
    node: str,
    service: str,
    ext: str,
    tld: bool,
) -> None:
    file: str = make_filename(node, service, ext, tld)
    assert helper.get_log_type(file) == service
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"log_type: {service} from {file}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_log_type_tmpdir(
    logger: pytest.LogCaptureFixture,
    make_filename: Callable[[str, str, str, bool], str],
    node: str,
    service: str,
    ext: str,
    tld: bool,
    tmpdir: pytest.TempdirFactory,
) -> None:
    file: str = make_filename(node, service, ext, tld)
    if os.path.splitext(file)[1] == ".log":
        filename: str = f"{tmpdir}{file[1:]}"
    else:
        filename = f"{tmpdir}{os.path.sep}{file}"
    assert helper.get_log_type(filename) == service
    assert logger.record_tuples == [
        (module_name, logging.DEBUG, f"log_type: {service} from {filename}")
    ]


@pytest.mark.parametrize("node, service, ext, tld", filename_data)
@pytest.mark.unit
def test_get_log_dir(
    logger: pytest.LogCaptureFixture,
    settings_override: config.Settings,
    node: str,
    service: str,
    ext: str,
    tld: bool,
) -> None:
    out: str = os.path.join(settings_override.outdir, node, service)
    test: str = helper.get_log_dir(node, service)
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

    def test_get_node(self) -> None:
        @given(file=st.text())
        def execute(**kwargs) -> None:
            helper.get_node(**kwargs)

        execute()

    def test_get_log_type(self) -> None:
        @given(file=st.text())
        def execute(**kwargs) -> None:
            helper.get_log_type(**kwargs)

        execute()

    def test_get_log_dir(self) -> None:
        @given(node=st.text(), log_type=st.text())
        def execute(**kwargs) -> None:
            helper.get_log_dir(**kwargs)

        execute()
