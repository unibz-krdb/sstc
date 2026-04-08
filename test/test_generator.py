import pytest

from sstc.guard import (
    GuardHierarchy,
    GuardLevel,
    build_cfd_where_branches,
    build_containment_pruning,
    build_null_pattern_where,
)


def _extract_section(result: str, start_marker: str, end_marker: str) -> str:
    start = result.index(start_marker)
    end = result.index(end_marker)
    return result[start:end]


def _assert_compile_structure(sql: str):
    assert "DROP SCHEMA IF EXISTS transducer CASCADE" in sql
    assert "CREATE SCHEMA transducer" in sql
    assert "CREATE TABLE transducer._loop" in sql

    create_count = sql.count("CREATE TABLE transducer.")
    assert create_count == 46, f"Expected 46 CREATE TABLE, got {create_count}"

    fn_count = sql.count("CREATE OR REPLACE FUNCTION")
    assert fn_count == 46, f"Expected 46 functions, got {fn_count}"

    trigger_count = sql.count("CREATE TRIGGER")
    assert trigger_count == 60, f"Expected 60 triggers, got {trigger_count}"

    assert "SOURCE_INSERT_FN" in sql
    assert "SOURCE_DELETE_FN" in sql
    assert "TARGET_INSERT_FN" in sql
    assert "TARGET_DELETE_FN" in sql

    assert "ON CONFLICT" in sql
    assert "DO NOTHING" in sql
    assert "NATURAL LEFT OUTER JOIN" in sql
    assert "ABS(loop_start)" in sql


def test_preamble(example_1_gen):
    preamble = example_1_gen._preamble()
    assert "DROP SCHEMA IF EXISTS transducer CASCADE" in preamble
    assert "CREATE SCHEMA transducer" in preamble
    assert "CREATE TABLE transducer._loop" in preamble
    assert "loop_start" in preamble


def test_compile_returns_string(example_1_gen):
    result = example_1_gen.compile()
    assert isinstance(result, str)
    assert len(result) > 0


def test_base_tables(example_1_gen):
    result = example_1_gen._base_tables()

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


def test_foreign_keys(example_1_gen):
    result = example_1_gen._foreign_keys()

    # 4 from equivalences + 3 from subsumptions = 7
    # (PEDDept->DeptManager skipped: dept is not PEDDept PK)
    # (Person_Source self-ref skipped: empid is not Person_Source PK)
    assert result.count("ADD FOREIGN KEY") == 7

    # Equivalence: PersonPhone.ssn -> Person.ssn
    assert (
        "ALTER TABLE transducer._personphone ADD FOREIGN KEY (ssn) REFERENCES transducer._person (ssn);"
        in result
    )

    # Subsumption: DeptManager.manager -> Employee.empid
    assert (
        "ALTER TABLE transducer._deptmanager ADD FOREIGN KEY (manager) REFERENCES transducer._employee (empid);"
        in result
    )


def test_mvd_constraints(example_1_gen):
    result = example_1_gen._constraints()

    # MVD check function (BEFORE INSERT)
    assert "check_person_source_mvd_check" in result.lower()
    assert "BEFORE INSERT" in result
    assert "EXCEPT" in result
    assert "RAISE EXCEPTION" in result

    # MVD grounding function (AFTER INSERT)
    assert "check_person_source_mvd_grounding" in result.lower()
    assert "AFTER INSERT" in result
    assert "UNION" in result


def test_fd_constraints(example_1_gen):
    result = example_1_gen._constraints()

    # 3 CFD check functions for person_source (each has function + trigger = 6)
    assert result.lower().count("check_person_source_cfd") == 6

    # All have exhaustive OR branches with IS NOT NULL
    assert "IS NOT NULL" in result

    # Contains RAISE EXCEPTION and BEFORE INSERT trigger
    assert result.count("RAISE EXCEPTION") >= 3
    assert "BEFORE INSERT" in result


def test_tracking_layer(example_1_gen):
    result = example_1_gen._tracking()

    # 9 tables x 2 (INSERT + DELETE) = 18 tracking tables
    assert result.count("CREATE TABLE") == 18

    # 9 x 2 = 18 capture functions
    assert result.count("CREATE OR REPLACE FUNCTION") == 18

    # 9 x 2 = 18 triggers
    assert result.count("CREATE TRIGGER") == 18

    # Source functions check loop_start = -1, target check loop_start = 1
    assert "loop_start = -1" in result
    assert "loop_start = 1" in result

    # Correct naming
    assert "_person_source_INSERT" in result
    assert "_person_source_DELETE" in result
    assert "_person_INSERT" in result


def test_join_layer(example_1_gen):
    result = example_1_gen._join()

    # 9 tables x 2 (INSERT_JOIN + DELETE_JOIN) = 18 staging tables
    assert result.count("CREATE TABLE") == 18

    # 9 x 2 = 18 join functions
    assert result.count("CREATE OR REPLACE FUNCTION") == 18

    # 9 x 2 = 18 join triggers
    assert result.count("CREATE TRIGGER") == 18

    # Source functions insert VALUES (1), target insert VALUES (-1)
    assert "VALUES (1)" in result
    assert "VALUES (-1)" in result

    # NATURAL LEFT OUTER JOIN used
    assert "NATURAL LEFT OUTER JOIN" in result

    # Temp table created with universal columns
    assert "CREATE TEMPORARY TABLE" in result


def test_source_insert_mapping(example_1_gen):
    result = example_1_gen._mapping()

    # Wait mechanism
    assert "ABS(loop_start)" in result

    # For each of 8 target tables: INSERT with ON CONFLICT DO NOTHING
    assert "ON CONFLICT" in result
    assert "DO NOTHING" in result

    # Cleanup DELETEs for source tracking tables
    assert "DELETE FROM" in result

    # Function name
    assert "SOURCE_INSERT_FN" in result


def test_target_insert_mapping(example_1_gen):
    result = example_1_gen._mapping()

    # Target insert mapping function
    assert "TARGET_INSERT_FN" in result

    # WHERE clause with IS NOT NULL for all universal columns
    assert "IS NOT NULL" in result

    # Insert into source tables
    assert "_person_source" in result


def test_source_delete_mapping(example_1_gen):
    result = example_1_gen._mapping()

    # Source delete function
    assert "SOURCE_DELETE_FN" in result
    assert "EXCEPT" in result


def test_target_delete_mapping(example_1_gen):
    result = example_1_gen._mapping()

    # Target delete function
    assert "TARGET_DELETE_FN" in result


def test_full_compile_structure(example_1_gen):
    sql = example_1_gen.compile()

    _assert_compile_structure(sql)

    # Foreign keys: 4 from inc= + 3 from inc subsumption = 7
    fk_count = sql.count("ADD FOREIGN KEY")
    assert fk_count == 7, f"Expected 7 foreign keys, got {fk_count}"


def test_example2_parses(example_2_ctx):
    assert len(example_2_ctx.source.tables) == 1
    assert len(example_2_ctx.target.tables) == 8


def test_guard_hierarchy_example1(example_1_gen):
    hierarchy = example_1_gen._build_guard_hierarchy()

    # example1: all columns nullable
    assert hierarchy.mandatory_cols == []
    assert set(hierarchy.nullable_cols) == {
        "ssn",
        "empid",
        "name",
        "hdate",
        "phone",
        "email",
        "dept",
        "manager",
    }

    # 3 distinct guard levels: {}, {empid,hdate}, {empid,hdate,dept,manager}
    assert len(hierarchy.levels) == 3
    assert hierarchy.levels[0].guard_attrs == set()
    assert hierarchy.levels[1].guard_attrs == {"empid", "hdate"}
    assert hierarchy.levels[2].guard_attrs == {"empid", "hdate", "dept", "manager"}


def test_guard_hierarchy_example2(example_2_gen):
    hierarchy = example_2_gen._build_guard_hierarchy()

    # example2: ssn, name, phone, email are NOT nullable
    assert set(hierarchy.mandatory_cols) == {"ssn", "name", "phone", "email"}
    assert set(hierarchy.nullable_cols) == {"empid", "hdate", "dept", "manager"}

    # Same 3 levels
    assert len(hierarchy.levels) == 3

    # Level 0: all nullable cols are NULL
    assert hierarchy.levels[0].null_cols == ["empid", "hdate", "dept", "manager"]
    assert hierarchy.levels[0].not_null_cols == []

    # Level 1: empid, hdate NOT NULL; dept, manager NULL
    assert set(hierarchy.levels[1].not_null_cols) == {"empid", "hdate"}
    assert set(hierarchy.levels[1].null_cols) == {"dept", "manager"}

    # Level 2: all NOT NULL
    assert set(hierarchy.levels[2].not_null_cols) == {
        "empid",
        "hdate",
        "dept",
        "manager",
    }
    assert hierarchy.levels[2].null_cols == []


def test_cfd_exhaustive_checks_example2(example_2_gen):
    result = example_2_gen._constraints()

    # 3 CFD check functions (guarded FDs -> CFD template)
    assert result.lower().count("check_person_source_cfd") == 6

    # CFD_1 (empid -> hdate, guard {empid, hdate}):
    assert "R2.empid IS NULL AND R2.hdate IS NOT NULL" in result
    assert "R2.empid IS NOT NULL AND R2.hdate IS NULL" in result

    # CFD_2 (empid -> dept, guard {empid, hdate, dept, manager}):
    assert "R2.empid IS NULL AND R2.dept IS NOT NULL" in result
    assert "R2.empid IS NULL AND R2.manager IS NOT NULL" in result
    assert "R2.dept IS NOT NULL AND R2.manager IS NULL" in result
    assert "R2.dept IS NULL AND R2.manager IS NOT NULL" in result

    # All use BEFORE INSERT triggers
    assert result.count("BEFORE INSERT") >= 3


def test_inc_constraint_example2(example_2_gen):
    result = example_2_gen._constraints()

    # INC enforcement function exists
    assert "check_person_source_inc" in result.lower()

    # Allows NULL manager
    assert "IS NULL" in result

    # Uses EXCEPT pattern for existence check
    assert "EXCEPT" in result

    # BEFORE INSERT trigger
    assert "BEFORE INSERT" in result


def test_conditional_inserts_example2(example_2_gen):
    result = example_2_gen._mapping()

    # Extract just the SOURCE_INSERT_FN section
    source_fn = _extract_section(result, "SOURCE_INSERT_FN", "TARGET_INSERT_FN")

    # Guarded tables should have IF EXISTS wrapping
    assert "IF EXISTS" in source_fn
    assert "empid IS NOT NULL AND hdate IS NOT NULL" in source_fn


def test_null_pattern_where_example2(example_2_gen):
    result = example_2_gen._mapping()

    # Extract TARGET_INSERT_FN section
    target_fn = _extract_section(result, "TARGET_INSERT_FN", "SOURCE_DELETE_FN")

    # Mandatory cols always NOT NULL
    assert "ssn IS NOT NULL" in target_fn
    assert "name IS NOT NULL" in target_fn
    assert "phone IS NOT NULL" in target_fn
    assert "email IS NOT NULL" in target_fn

    # Null-pattern disjunction (not all-NOT-NULL)
    assert "empid IS NULL AND hdate IS NULL" in target_fn
    assert "empid IS NOT NULL AND hdate IS NOT NULL" in target_fn


def test_tuple_containment_pruning_example2(example_2_gen):
    result = example_2_gen._mapping()

    # Extract TARGET_INSERT_FN section
    target_fn = _extract_section(result, "TARGET_INSERT_FN", "SOURCE_DELETE_FN")

    # Tuple containment pruning should appear AFTER temp_table_join INSERT
    assert "DELETE FROM temp_table_join" in target_fn

    # Should check for richer tuples at Level 1 (empid, hdate non-null)
    assert "empid IS NOT NULL AND hdate IS NOT NULL" in target_fn

    # Should delete poorer tuples where nullable cols are NULL
    assert "empid IS NULL" in target_fn


def test_full_compile_example2(example_2_gen):
    sql = example_2_gen.compile()

    _assert_compile_structure(sql)

    # Composite PK on source
    assert "PRIMARY KEY (ssn, phone, email)" in sql

    # NOT NULL on mandatory columns
    assert "ssn VARCHAR(100) NOT NULL" in sql
    assert "name VARCHAR(100) NOT NULL" in sql

    # Key patterns from design
    assert "IF EXISTS" in sql  # Conditional INSERTs
    assert "empid IS NULL AND hdate IS NULL" in sql  # Null-pattern WHERE


def test_null_pattern_where_example1_requires_pk_not_null(example_1_gen):
    """Regression: all-nullable schema must require source PK NOT NULL in WHERE."""
    hierarchy = example_1_gen._build_guard_hierarchy()
    where = build_null_pattern_where(hierarchy)

    # Source PK must always be NOT NULL
    assert where.startswith("ssn IS NOT NULL")

    # ssn must not appear as IS NULL anywhere in the WHERE
    assert "ssn IS NULL" not in where


# --- Unit tests for _build_null_pattern_where ---


def test_null_pattern_where_all_mandatory():
    """All mandatory cols, no nullable -> just NOT NULL conjunction."""
    h = GuardHierarchy(
        mandatory_cols=["a", "b"],
        nullable_cols=[],
        levels=[GuardLevel(guard_attrs=set(), not_null_cols=[], null_cols=[])],
        source_pk=["a"],
    )
    result = build_null_pattern_where(h)
    assert result == "a IS NOT NULL AND b IS NOT NULL"


def test_null_pattern_where_mixed():
    """Mandatory prefix + disjunction for nullable cols."""
    h = GuardHierarchy(
        mandatory_cols=["ssn", "name"],
        nullable_cols=["empid", "hdate"],
        levels=[
            GuardLevel(
                guard_attrs=set(), not_null_cols=[], null_cols=["empid", "hdate"]
            ),
            GuardLevel(
                guard_attrs={"empid", "hdate"},
                not_null_cols=["empid", "hdate"],
                null_cols=[],
            ),
        ],
        source_pk=["ssn"],
    )
    result = build_null_pattern_where(h)
    assert result.startswith("ssn IS NOT NULL AND name IS NOT NULL")
    assert "(empid IS NULL AND hdate IS NULL)" in result
    assert "(empid IS NOT NULL AND hdate IS NOT NULL)" in result


def test_null_pattern_where_all_nullable_uses_source_pk():
    """All nullable schema: source_pk used as identity prefix, excluded from branches."""
    h = GuardHierarchy(
        mandatory_cols=[],
        nullable_cols=["pk1", "a", "b"],
        levels=[
            GuardLevel(
                guard_attrs=set(), not_null_cols=[], null_cols=["pk1", "a", "b"]
            ),
            GuardLevel(
                guard_attrs={"a", "b"}, not_null_cols=["a", "b"], null_cols=["pk1"]
            ),
        ],
        source_pk=["pk1"],
    )
    result = build_null_pattern_where(h)
    assert result.startswith("pk1 IS NOT NULL")
    assert "pk1 IS NULL" not in result
    assert "(a IS NULL AND b IS NULL)" in result
    assert "(a IS NOT NULL AND b IS NOT NULL)" in result


def test_null_pattern_where_single_level():
    """Single level -> one-branch disjunction."""
    h = GuardHierarchy(
        mandatory_cols=["pk"],
        nullable_cols=["x"],
        levels=[GuardLevel(guard_attrs=set(), not_null_cols=[], null_cols=["x"])],
        source_pk=["pk"],
    )
    result = build_null_pattern_where(h)
    assert result == "pk IS NOT NULL AND ((x IS NULL))"


# --- Unit tests for _build_cfd_where_branches ---


def test_cfd_branches_simple_2attr_guard():
    """empid -> hdate with guard {empid, hdate} -> exactly 3 branches."""
    h = GuardHierarchy(
        mandatory_cols=[],
        nullable_cols=["empid", "hdate"],
        levels=[
            GuardLevel(
                guard_attrs=set(), not_null_cols=[], null_cols=["empid", "hdate"]
            ),
            GuardLevel(
                guard_attrs={"empid", "hdate"},
                not_null_cols=["empid", "hdate"],
                null_cols=[],
            ),
        ],
        source_pk=["ssn"],
    )
    branches = build_cfd_where_branches(
        lhs_attrs=["empid"],
        rhs_attrs=["hdate"],
        guard_attrs=["empid", "hdate"],
        hierarchy=h,
    )
    assert len(branches) == 3
    assert "R1.empid = R2.empid" in branches[0]
    assert "R1.hdate <> R2.hdate" in branches[0]
    assert "(R2.empid IS NULL AND R2.hdate IS NOT NULL)" in branches
    assert "(R2.empid IS NOT NULL AND R2.hdate IS NULL)" in branches


def test_cfd_branches_complex_4attr_guard():
    """empid -> dept with guard {empid, hdate, dept, manager} -> 5 branches.

    Matches reference SQL: 1 main + 2 cross-level + 2 coherence.
    """
    h = GuardHierarchy(
        mandatory_cols=[],
        nullable_cols=["empid", "hdate", "dept", "manager"],
        levels=[
            GuardLevel(
                guard_attrs=set(),
                not_null_cols=[],
                null_cols=["empid", "hdate", "dept", "manager"],
            ),
            GuardLevel(
                guard_attrs={"empid", "hdate"},
                not_null_cols=["empid", "hdate"],
                null_cols=["dept", "manager"],
            ),
            GuardLevel(
                guard_attrs={"empid", "hdate", "dept", "manager"},
                not_null_cols=["empid", "hdate", "dept", "manager"],
                null_cols=[],
            ),
        ],
        source_pk=["ssn"],
    )
    branches = build_cfd_where_branches(
        lhs_attrs=["empid"],
        rhs_attrs=["dept"],
        guard_attrs=["empid", "hdate", "dept", "manager"],
        hierarchy=h,
    )
    assert len(branches) == 5
    assert "R1.empid = R2.empid AND R1.dept <> R2.dept" in branches[0]
    # Cross-level: LHS NULL -> no RHS-group attr NOT NULL
    assert "(R2.empid IS NULL AND R2.dept IS NOT NULL)" in branches
    assert "(R2.empid IS NULL AND R2.manager IS NOT NULL)" in branches
    # Coherence: dept and manager must be jointly defined
    assert (
        "(R2.empid IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NULL)"
        in branches
    )
    assert (
        "(R2.empid IS NOT NULL AND R2.dept IS NULL AND R2.manager IS NOT NULL)"
        in branches
    )


def test_cfd_branches_no_duplicates():
    """No duplicate branches regardless of attr overlap."""
    h = GuardHierarchy(
        mandatory_cols=[],
        nullable_cols=["a", "b"],
        levels=[
            GuardLevel(guard_attrs=set(), not_null_cols=[], null_cols=["a", "b"]),
            GuardLevel(guard_attrs={"a", "b"}, not_null_cols=["a", "b"], null_cols=[]),
        ],
        source_pk=["pk"],
    )
    branches = build_cfd_where_branches(
        lhs_attrs=["a"], rhs_attrs=["b"], guard_attrs=["a", "b"], hierarchy=h
    )
    assert len(branches) == len(set(branches))


# --- Unit tests for _build_containment_pruning ---


def test_containment_pruning_multi_level(example_1_gen):
    """3 levels -> 2 pruning rules, identity uses source_pk."""
    hierarchy = example_1_gen._build_guard_hierarchy()
    rules = build_containment_pruning(hierarchy)

    assert len(rules) == 2
    assert "t_rich.empid IS NOT NULL" in rules[0]["richer_condition"]
    assert "t_rich.hdate IS NOT NULL" in rules[0]["richer_condition"]
    assert "t_poor.empid IS NULL" in rules[0]["poorer_condition"]
    assert "empid IS NOT NULL" in rules[0]["richer_check"]
    assert rules[0]["identity_match"] == "t_rich.ssn = t_poor.ssn"


def test_containment_pruning_single_level():
    """Single hierarchy level -> no pruning rules."""
    h = GuardHierarchy(
        mandatory_cols=["pk"],
        nullable_cols=["x"],
        levels=[GuardLevel(guard_attrs=set(), not_null_cols=[], null_cols=["x"])],
        source_pk=["pk"],
    )
    assert build_containment_pruning(h) == []


def test_containment_pruning_no_nullable():
    """No nullable columns -> no pruning needed."""
    h = GuardHierarchy(
        mandatory_cols=["a", "b"],
        nullable_cols=[],
        levels=[
            GuardLevel(guard_attrs=set(), not_null_cols=[], null_cols=[]),
            GuardLevel(guard_attrs={"a"}, not_null_cols=[], null_cols=[]),
        ],
        source_pk=["a"],
    )
    assert build_containment_pruning(h) == []


def test_containment_pruning_identity_uses_mandatory(example_2_gen):
    """When mandatory_cols is non-empty, identity_match uses mandatory_cols."""
    hierarchy = example_2_gen._build_guard_hierarchy()
    rules = build_containment_pruning(hierarchy)

    assert len(rules) == 2
    for rule in rules:
        assert "t_rich.ssn = t_poor.ssn" in rule["identity_match"]
        assert "t_rich.name = t_poor.name" in rule["identity_match"]
        assert "t_rich.phone = t_poor.phone" in rule["identity_match"]
        assert "t_rich.email = t_poor.email" in rule["identity_match"]


# --- Parametrized tests for _extract_table_guard_attrs ---


@pytest.mark.parametrize(
    "table_name,expected",
    [
        ("employee", {"empid", "hdate"}),
        ("person", set()),
    ],
)
def test_extract_guard_attrs(example_1_gen, example_1_ctx, table_name, expected):
    table = next(t for t in example_1_ctx.target.tables if t.name == table_name)
    assert set(example_1_gen._extract_table_guard_attrs(table)) == expected


# --- Parametrized tests for _inc_sql ---


@pytest.mark.parametrize(
    "context_attr,expect_empty",
    [
        ("source", False),
        ("target", True),
    ],
)
def test_inc_sql(example_1_gen, example_1_ctx, context_attr, expect_empty):
    ctx = getattr(example_1_ctx, context_attr)
    result = example_1_gen._inc_sql(ctx)
    if expect_empty:
        assert result == ""
    else:
        assert result != ""
        assert "check_person_source_inc_1_fn" in result
        assert "BEFORE INSERT" in result
        assert "EXCEPT" in result
