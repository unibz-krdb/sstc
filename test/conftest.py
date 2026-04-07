import os

import pytest

from sstc import TransducerContext
from sstc.generator import Generator


def _example_dir(name: str) -> str:
    path = os.path.join("test", "inputs", name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} does not exist.")
    return path


@pytest.fixture
def example_1_dir():
    return _example_dir("example1")


@pytest.fixture
def example_2_dir():
    return _example_dir("example2")


@pytest.fixture
def example_1_ctx(example_1_dir):
    return TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )


@pytest.fixture
def example_2_ctx(example_2_dir):
    return TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )


@pytest.fixture
def example_1_gen(example_1_ctx):
    return Generator(example_1_ctx)


@pytest.fixture
def example_2_gen(example_2_ctx):
    return Generator(example_2_ctx)
