"""Integration tests: compile example1, install on Postgres, verify propagation."""

import pytest

pytestmark = pytest.mark.integration


# --- Smoke test ---


def test_schema_installs(transducer_db):
    """Compiled SQL installs without error; all expected tables exist."""
    rows = transducer_db.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'transducer'
        """
    ).fetchall()
    tables = {r[0] for r in rows}

    expected_base = {
        "_loop",
        "_person_source",
        "_person",
        "_personphone",
        "_personemail",
        "_employee",
        "_employeedate",
        "_ped",
        "_peddept",
        "_deptmanager",
    }
    assert expected_base.issubset(tables), f"Missing: {expected_base - tables}"
    # 10 base + 18 tracking (_INSERT/_DELETE) + 18 join (_INSERT_JOIN/_DELETE_JOIN)
    assert len(tables) == 46


# --- Source-to-target propagation ---


def test_simple_person_propagates(transducer_db):
    """Level 0: person with only ssn/name/phone/email (no employee info)."""
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('S1', NULL, 'Alice', NULL, 'P1', 'E1', NULL, NULL)
        """
    )

    # Level 0 targets populated
    assert transducer_db.execute(
        "SELECT * FROM transducer._person"
    ).fetchall() == [("S1", "Alice")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._personphone"
    ).fetchall() == [("S1", "P1")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._personemail"
    ).fetchall() == [("S1", "E1")]

    # Level 1 + 2 targets empty
    assert transducer_db.execute(
        "SELECT * FROM transducer._employee"
    ).fetchall() == []
    assert transducer_db.execute(
        "SELECT * FROM transducer._employeedate"
    ).fetchall() == []
    assert transducer_db.execute(
        "SELECT * FROM transducer._ped"
    ).fetchall() == []
    assert transducer_db.execute(
        "SELECT * FROM transducer._peddept"
    ).fetchall() == []
    assert transducer_db.execute(
        "SELECT * FROM transducer._deptmanager"
    ).fetchall() == []

    # Tracking tables cleaned up
    assert transducer_db.execute(
        "SELECT * FROM transducer._person_source_insert"
    ).fetchall() == []
    assert transducer_db.execute(
        "SELECT * FROM transducer._person_source_insert_join"
    ).fetchall() == []
    assert transducer_db.execute(
        "SELECT * FROM transducer._loop"
    ).fetchall() == []


def test_employee_propagates(transducer_db):
    """Level 1: employee with empid+hdate, no department."""
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('S2', 'EMP2', 'Bob', 'H2', 'P2', 'E2', NULL, NULL)
        """
    )

    # Level 0
    assert transducer_db.execute(
        "SELECT * FROM transducer._person"
    ).fetchall() == [("S2", "Bob")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._personphone"
    ).fetchall() == [("S2", "P2")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._personemail"
    ).fetchall() == [("S2", "E2")]

    # Level 1
    assert transducer_db.execute(
        "SELECT * FROM transducer._employee"
    ).fetchall() == [("S2", "EMP2")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._employeedate"
    ).fetchall() == [("EMP2", "H2")]

    # Level 2 empty
    assert transducer_db.execute(
        "SELECT * FROM transducer._ped"
    ).fetchall() == []
    assert transducer_db.execute(
        "SELECT * FROM transducer._peddept"
    ).fetchall() == []
    assert transducer_db.execute(
        "SELECT * FROM transducer._deptmanager"
    ).fetchall() == []


def test_full_employee_with_dept_propagates(transducer_db):
    """Level 2: full tuple, all 8 target tables populated.

    manager=empid (self-managing) satisfies the INC constraint
    inc⊆_{manager, empid}, and the FK _deptmanager(manager) → _employee(empid).
    """
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('S3', 'EMP3', 'Carol', 'H3', 'P3', 'E3', 'D3', 'EMP3')
        """
    )

    # Level 0
    assert transducer_db.execute(
        "SELECT * FROM transducer._person"
    ).fetchall() == [("S3", "Carol")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._personphone"
    ).fetchall() == [("S3", "P3")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._personemail"
    ).fetchall() == [("S3", "E3")]

    # Level 1
    assert transducer_db.execute(
        "SELECT * FROM transducer._employee"
    ).fetchall() == [("S3", "EMP3")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._employeedate"
    ).fetchall() == [("EMP3", "H3")]

    # Level 2
    assert transducer_db.execute(
        "SELECT * FROM transducer._ped"
    ).fetchall() == [("S3", "EMP3")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._peddept"
    ).fetchall() == [("EMP3", "D3")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._deptmanager"
    ).fetchall() == [("D3", "EMP3")]


def test_multiple_persons_propagate(transducer_db):
    """Three inserts at different guard levels; verify independent propagation."""
    # Level 0 only
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source VALUES
            ('S10', NULL, 'Xena', NULL, 'P10', 'E10', NULL, NULL)
        """
    )
    # Level 0 + 1
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source VALUES
            ('S20', 'EMP20', 'Yara', 'H20', 'P20', 'E20', NULL, NULL)
        """
    )
    # Level 0 + 1 + 2 (manager = EMP20, an existing empid, for INC)
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source VALUES
            ('S30', 'EMP30', 'Zara', 'H30', 'P30', 'E30', 'D30', 'EMP20')
        """
    )

    # _person: 3 rows
    person = transducer_db.execute(
        "SELECT * FROM transducer._person ORDER BY ssn"
    ).fetchall()
    assert len(person) == 3
    assert {r[0] for r in person} == {"S10", "S20", "S30"}

    # _personphone / _personemail: 3 each
    assert len(transducer_db.execute(
        "SELECT * FROM transducer._personphone"
    ).fetchall()) == 3
    assert len(transducer_db.execute(
        "SELECT * FROM transducer._personemail"
    ).fetchall()) == 3

    # _employee: 2 (S20, S30)
    employee = transducer_db.execute(
        "SELECT * FROM transducer._employee ORDER BY empid"
    ).fetchall()
    assert len(employee) == 2
    assert {r[1] for r in employee} == {"EMP20", "EMP30"}

    # _employeedate: 2
    assert len(transducer_db.execute(
        "SELECT * FROM transducer._employeedate"
    ).fetchall()) == 2

    # _ped / _peddept / _deptmanager: 1 each (S30 only)
    assert transducer_db.execute(
        "SELECT * FROM transducer._ped"
    ).fetchall() == [("S30", "EMP30")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._peddept"
    ).fetchall() == [("EMP30", "D30")]
    assert transducer_db.execute(
        "SELECT * FROM transducer._deptmanager"
    ).fetchall() == [("D30", "EMP20")]

    # Tracking fully cleaned
    assert transducer_db.execute(
        "SELECT * FROM transducer._loop"
    ).fetchall() == []
