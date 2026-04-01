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


def test_base_tables(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._base_tables()

    # All 9 tables created
    assert result.count("CREATE TABLE") == 9

    # Source table with PK
    assert "CREATE TABLE transducer._person_source" in result
    assert "PRIMARY KEY (ssn)" in result

    # Target tables present
    for name in [
        "person",
        "personphone",
        "personemail",
        "employee",
        "employeedate",
        "ped",
        "peddept",
        "deptmanager",
    ]:
        assert f"CREATE TABLE transducer._{name}" in result


def test_mvd_constraints(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._constraints()

    # MVD check function (BEFORE INSERT)
    assert "check_person_source_mvd_check" in result.lower()
    assert "BEFORE INSERT" in result
    assert "EXCEPT" in result
    assert "RAISE EXCEPTION" in result

    # MVD grounding function (AFTER INSERT)
    assert "check_person_source_mvd_grounding" in result.lower()
    assert "AFTER INSERT" in result
    assert "UNION" in result


def test_fd_constraints(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._constraints()

    # 3 FD check functions for person_source (each has function + trigger = 6 occurrences)
    assert result.lower().count("check_person_source_fd") == 6

    # All guarded with IS NOT NULL
    assert "IS NOT NULL" in result

    # Contains RAISE EXCEPTION and BEFORE INSERT trigger
    assert result.count("RAISE EXCEPTION") >= 3  # at least from FDs
    assert "BEFORE INSERT" in result


def test_tracking_layer(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._tracking()

    # 9 tables × 2 (INSERT + DELETE) = 18 tracking tables
    assert result.count("CREATE TABLE") == 18

    # 9 × 2 = 18 capture functions
    assert result.count("CREATE OR REPLACE FUNCTION") == 18

    # 9 × 2 = 18 triggers
    assert result.count("CREATE TRIGGER") == 18

    # Source functions check loop_start = -1, target check loop_start = 1
    assert "loop_start = -1" in result
    assert "loop_start = 1" in result

    # Correct naming
    assert "_person_source_INSERT" in result
    assert "_person_source_DELETE" in result
    assert "_person_INSERT" in result


def test_join_layer(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._join()

    # 9 tables × 2 (INSERT_JOIN + DELETE_JOIN) = 18 staging tables
    assert result.count("CREATE TABLE") == 18

    # 9 × 2 = 18 join functions
    assert result.count("CREATE OR REPLACE FUNCTION") == 18

    # 9 × 2 = 18 join triggers
    assert result.count("CREATE TRIGGER") == 18

    # Source functions insert VALUES (1), target insert VALUES (-1)
    assert "VALUES (1)" in result
    assert "VALUES (-1)" in result

    # NATURAL LEFT OUTER JOIN used
    assert "NATURAL LEFT OUTER JOIN" in result

    # Temp table created with universal columns
    assert "CREATE TEMPORARY TABLE" in result
