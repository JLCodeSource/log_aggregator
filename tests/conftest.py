"""
This module contains shared fixtures, steps and hooks.
"""
import pytest
import logging


@pytest.fixture()
def logger(caplog):
    caplog.set_level(logging.DEBUG)
    return caplog
