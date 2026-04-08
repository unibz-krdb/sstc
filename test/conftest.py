import os

import pytest

from sstc import TransducerContext
from sstc.generator import Generator


def pytest_addoption(parser):
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Regenerate golden files instead of comparing against them.",
    )


@pytest.fixture
def update_golden(request):
    return request.config.getoption("--update-golden")


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


# ---------------------------------------------------------------------------
# Integration test fixtures (require testcontainers + psycopg)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_container():
    pytest.importorskip("testcontainers", reason="testcontainers not installed")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:17", driver=None) as pg:
        yield pg


@pytest.fixture
def pg_conn(pg_container):
    import psycopg

    conn = psycopg.connect(pg_container.get_connection_url(), autocommit=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def example1_sql():
    ctx = TransducerContext.from_files(
        universal_path=os.path.join("test", "inputs", "example1", "universal.json"),
        source_path=os.path.join("test", "inputs", "example1", "source.txt"),
        target_path=os.path.join("test", "inputs", "example1", "target.txt"),
    )
    return Generator(ctx).compile()


@pytest.fixture
def transducer_db(pg_conn, example1_sql):
    pg_conn.execute(example1_sql)
    return pg_conn
