import os

from fixtures import example_1_dir as example_1_dir
from fixtures import example_2_dir as example_2_dir

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


def test_foreign_keys(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._foreign_keys()

    # 4 from equivalences + 3 from subsumptions = 7
    # (PEDDept→DeptManager skipped: dept is not PEDDept PK)
    # (Person_Source self-ref skipped: empid is not Person_Source PK)
    assert result.count("ADD FOREIGN KEY") == 7

    # Equivalence: PersonPhone.ssn → Person.ssn
    assert (
        "ALTER TABLE transducer._personphone ADD FOREIGN KEY (ssn) REFERENCES transducer._person (ssn);"
        in result
    )

    # Subsumption: DeptManager.manager → Employee.empid
    assert (
        "ALTER TABLE transducer._deptmanager ADD FOREIGN KEY (manager) REFERENCES transducer._employee (empid);"
        in result
    )


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


def test_source_insert_mapping(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._mapping()

    # Wait mechanism
    assert "ABS(loop_start)" in result

    # For each of 8 target tables: INSERT with ON CONFLICT DO NOTHING
    assert "ON CONFLICT" in result
    assert "DO NOTHING" in result

    # Cleanup DELETEs for source tracking tables
    assert "DELETE FROM" in result

    # Function name
    assert "SOURCE_INSERT_FN" in result


def test_target_insert_mapping(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._mapping()

    # Target insert mapping function
    assert "TARGET_INSERT_FN" in result

    # WHERE clause with IS NOT NULL for all universal columns
    assert "IS NOT NULL" in result

    # Insert into source tables
    assert "_person_source" in result


def test_source_delete_mapping(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._mapping()

    # Source delete function
    assert "SOURCE_DELETE_FN" in result
    assert "EXCEPT" in result


def test_target_delete_mapping(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._mapping()

    # Target delete function
    assert "TARGET_DELETE_FN" in result


def test_full_compile_structure(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    sql = Generator(ctx).compile()

    # Schema infrastructure
    assert "DROP SCHEMA IF EXISTS transducer CASCADE" in sql
    assert "CREATE SCHEMA transducer" in sql
    assert "CREATE TABLE transducer._loop" in sql

    # Base tables: 1 source + 8 target = 9
    # Tracking: 9 × 2 (_INSERT, _DELETE) = 18
    # Join staging: 9 × 2 (_INSERT_JOIN, _DELETE_JOIN) = 18
    # Total CREATE TABLE: 9 + 18 + 18 + 1 (loop) = 46
    create_table_count = sql.count("CREATE TABLE transducer.")
    assert create_table_count == 46, (
        f"Expected 46 CREATE TABLE, got {create_table_count}"
    )

    # Functions: 2 MVD + 3 FD + 18 capture + 18 join + 4 mapping = 45
    fn_count = sql.count("CREATE OR REPLACE FUNCTION")
    assert fn_count == 45, f"Expected 45 functions, got {fn_count}"

    # Triggers: 2 MVD + 3 FD + 18 capture + 18 join + 18 mapping = 59
    trigger_count = sql.count("CREATE TRIGGER")
    assert trigger_count == 59, f"Expected 59 triggers, got {trigger_count}"

    # Mapping functions present
    assert "SOURCE_INSERT_FN" in sql
    assert "SOURCE_DELETE_FN" in sql
    assert "TARGET_INSERT_FN" in sql
    assert "TARGET_DELETE_FN" in sql

    # Foreign keys: 4 from inc= + 3 from inc⊆ = 7
    fk_count = sql.count("ADD FOREIGN KEY")
    assert fk_count == 7, f"Expected 7 foreign keys, got {fk_count}"

    # Key SQL patterns
    assert "ON CONFLICT" in sql
    assert "DO NOTHING" in sql
    assert "NATURAL LEFT OUTER JOIN" in sql
    assert "ABS(loop_start)" in sql


def test_example2_parses(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    assert len(ctx.source.tables) == 1
    assert len(ctx.target.tables) == 8
