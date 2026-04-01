import os

from fixtures import example_1_dir as example_1_dir

from sstc import TransducerContext
from sstc.generator import Generator


def test_preamble(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    preamble = gen._preamble()
    assert "DROP SCHEMA IF EXISTS transducer CASCADE" in preamble
    assert "CREATE SCHEMA transducer" in preamble
    assert "CREATE TABLE transducer._loop" in preamble
    assert "loop_start" in preamble


def test_compile_returns_string(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    result = Generator(ctx).compile()
    assert isinstance(result, str)
    assert len(result) > 0
