"""
Unit and regression test for the eden package.
"""

# Import package, test suite, and other packages as needed
import sys

import pytest

import eden


def test_eden_imported():
    """Sample test, will always pass so long as import statement worked."""
    assert "eden" in sys.modules
