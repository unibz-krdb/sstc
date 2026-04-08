"""Integration tests: compile example1, install on Postgres, verify propagation."""

import psycopg.errors
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


# --- Constraint enforcement (Phase D) ---


def test_cfd_empid_hdate_violation(transducer_db):
    """CFD empid→hdate: empid non-null with hdate null rejected (guard incoherence).

    NOTE: CFD triggers compare NEW against existing rows via cross join, so they
    require at least one row in _person_source to fire.  The guard hierarchy still
    prevents incorrect propagation for the very first insert.
    """
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('V0', NULL, 'Seed', NULL, 'P0', 'E0', NULL, NULL)
        """
    )
    with pytest.raises(
        psycopg.errors.RaiseException, match="CFD violation.*empid -> hdate"
    ):
        transducer_db.execute(
            """
            INSERT INTO transducer._person_source
                (ssn, empid, name, hdate, phone, email, dept, manager)
            VALUES ('V1', 'EMP_V1', 'Vicky', NULL, 'PV1', 'EV1', NULL, NULL)
            """
        )


def test_cfd_empid_dept_cross_level_violation(transducer_db):
    """CFD empid→dept: dept non-null without empid rejected (cross-level coherence)."""
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('V0', NULL, 'Seed', NULL, 'P0', 'E0', NULL, NULL)
        """
    )
    with pytest.raises(
        psycopg.errors.RaiseException, match="CFD violation.*empid -> dept"
    ):
        transducer_db.execute(
            """
            INSERT INTO transducer._person_source
                (ssn, empid, name, hdate, phone, email, dept, manager)
            VALUES ('V2', NULL, 'Wade', NULL, 'PV2', 'EV2', 'DV2', NULL)
            """
        )


def test_cfd_dept_manager_violation(transducer_db):
    """CFD dept→manager: same dept with different managers rejected (FD conflict)."""
    # First employee: self-managing, dept=DEPTX
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('VA', 'EMPA', 'Amy', 'HA', 'PA', 'EA', 'DEPTX', 'EMPA')
        """
    )
    # Second employee: no dept (needed so EMPB exists as empid for INC)
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('VB', 'EMPB', 'Ben', 'HB', 'PB', 'EB', NULL, NULL)
        """
    )
    # Third: same dept=DEPTX but manager=EMPB (conflicts with EMPA for DEPTX)
    with pytest.raises(
        psycopg.errors.RaiseException, match="CFD violation.*dept -> manager"
    ):
        transducer_db.execute(
            """
            INSERT INTO transducer._person_source
                (ssn, empid, name, hdate, phone, email, dept, manager)
            VALUES ('VC', 'EMPC', 'Cal', 'HC', 'PC', 'EC', 'DEPTX', 'EMPB')
            """
        )


def test_inc_violation(transducer_db):
    """INC manager⊆empid: manager referencing non-existent empid rejected."""
    # Need at least one existing row so the INC SELECT returns something
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('VA', NULL, 'Amy', NULL, 'PA', 'EA', NULL, NULL)
        """
    )
    with pytest.raises(
        psycopg.errors.RaiseException, match="INC violation"
    ):
        transducer_db.execute(
            """
            INSERT INTO transducer._person_source
                (ssn, empid, name, hdate, phone, email, dept, manager)
            VALUES ('VB', 'EMPB', 'Ben', 'HB', 'PB', 'EB', 'DB', 'GHOST')
            """
        )


def test_mvd_violation(transducer_db):
    """MVD {ssn}→→{phone}: inconsistent non-MVD attrs for same ssn rejected."""
    transducer_db.execute(
        """
        INSERT INTO transducer._person_source
            (ssn, empid, name, hdate, phone, email, dept, manager)
        VALUES ('VM', NULL, 'Mary', NULL, 'PM1', 'EM1', NULL, NULL)
        """
    )
    # Same ssn, different name → cross-product tuple doesn't exist → violation
    with pytest.raises(
        psycopg.errors.RaiseException, match="MVD constraint violation"
    ):
        transducer_db.execute(
            """
            INSERT INTO transducer._person_source
                (ssn, empid, name, hdate, phone, email, dept, manager)
            VALUES ('VM', NULL, 'Nora', NULL, 'PM2', 'EM2', NULL, NULL)
            """
        )


# --- Target-to-source propagation (Phase C) ---
#
# Protocol: INSERT a seed into _loop with value = 1 + N where N is the number
# of target table inserts that follow.  Each target insert's join function adds
# -1 to _loop; when count reaches ABS(seed), TARGET_INSERT_FN fires and
# reconstructs the universal tuple into _person_source.



def test_target_to_source_simple_person(transducer_db):
    """Insert a Level 0 person via target tables; verify _person_source populated."""
    # 3 target inserts → seed = 4
    transducer_db.execute("INSERT INTO transducer._loop VALUES (4)")
    transducer_db.execute(
        "INSERT INTO transducer._person VALUES ('T1', 'Dana')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._personphone VALUES ('T1', 'TP1')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._personemail VALUES ('T1', 'TE1')"
    )

    source = transducer_db.execute(
        "SELECT * FROM transducer._person_source"
    ).fetchall()
    assert len(source) == 1, f"Expected 1 row in _person_source, got {len(source)}"
    assert source[0] == ("T1", None, "Dana", None, "TP1", "TE1", None, None)

    # Tracking cleaned up
    assert transducer_db.execute(
        "SELECT * FROM transducer._loop"
    ).fetchall() == []


def test_target_to_source_employee(transducer_db):
    """Insert a Level 1 employee via target tables; verify _person_source populated."""
    # 5 target inserts → seed = 6
    transducer_db.execute("INSERT INTO transducer._loop VALUES (6)")
    transducer_db.execute(
        "INSERT INTO transducer._person VALUES ('T2', 'Eve')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._employee VALUES ('T2', 'TEMP2')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._employeedate VALUES ('TEMP2', 'TH2')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._personphone VALUES ('T2', 'TP2')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._personemail VALUES ('T2', 'TE2')"
    )

    source = transducer_db.execute(
        "SELECT * FROM transducer._person_source"
    ).fetchall()
    assert len(source) == 1, f"Expected 1 row in _person_source, got {len(source)}"
    assert source[0] == ("T2", "TEMP2", "Eve", "TH2", "TP2", "TE2", None, None)

    assert transducer_db.execute(
        "SELECT * FROM transducer._loop"
    ).fetchall() == []


def test_target_to_source_full_employee(transducer_db):
    """Insert a Level 2 full employee via target tables; verify _person_source populated."""
    # 8 target inserts → seed = 9
    transducer_db.execute("INSERT INTO transducer._loop VALUES (9)")
    transducer_db.execute(
        "INSERT INTO transducer._person VALUES ('T3', 'Finn')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._employee VALUES ('T3', 'TEMP3')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._employeedate VALUES ('TEMP3', 'TH3')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._ped VALUES ('T3', 'TEMP3')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._deptmanager VALUES ('TD3', 'TEMP3')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._peddept VALUES ('TEMP3', 'TD3')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._personphone VALUES ('T3', 'TP3')"
    )
    transducer_db.execute(
        "INSERT INTO transducer._personemail VALUES ('T3', 'TE3')"
    )

    source = transducer_db.execute(
        "SELECT * FROM transducer._person_source"
    ).fetchall()
    assert len(source) == 1, f"Expected 1 row in _person_source, got {len(source)}"
    assert source[0] == (
        "T3", "TEMP3", "Finn", "TH3", "TP3", "TE3", "TD3", "TEMP3"
    )

    assert transducer_db.execute(
        "SELECT * FROM transducer._loop"
    ).fetchall() == []
