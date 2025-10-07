import os
import pytest


@pytest.fixture
def example_1_dir():
    path = os.path.join("test", "inputs", "example1")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} does not exist.")
    return path
