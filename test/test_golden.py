import os

import pytest

from sstc import TransducerContext
from sstc.generator import Generator

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")


def _compile_example(name: str) -> str:
    input_dir = os.path.join("test", "inputs", name)
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(input_dir, "universal.json"),
        source_path=os.path.join(input_dir, "source.txt"),
        target_path=os.path.join(input_dir, "target.txt"),
    )
    return Generator(ctx).compile()


@pytest.mark.parametrize("example", ["example1", "example2"])
def test_golden(example, update_golden):
    actual = _compile_example(example)
    golden_path = os.path.join(GOLDEN_DIR, f"{example}.sql")

    if update_golden:
        os.makedirs(GOLDEN_DIR, exist_ok=True)
        with open(golden_path, "w") as f:
            f.write(actual)
        return

    with open(golden_path, "r") as f:
        expected = f.read()

    assert actual == expected, (
        f"Golden file mismatch for {example}. Run with --update-golden to regenerate."
    )
