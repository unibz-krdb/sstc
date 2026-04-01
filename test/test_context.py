import os

from fixtures import example_1_dir as example_1_dir

from rapt2.treebrd.node import (
    FunctionalDependencyNode,
    InclusionEquivalenceNode,
    InclusionSubsumptionNode,
    MultivaluedDependencyNode,
    PrimaryKeyNode,
)

from sstc import TransducerContext


def test_context(example_1_dir: str):
    transducer_ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )

    source_ctx = transducer_ctx.source
    assert len(source_ctx.tables) == 1
    assert source_ctx.schema.to_dict() == {
        "person_source": [
            "ssn",
            "empid",
            "name",
            "hdate",
            "phone",
            "email",
            "dept",
            "manager",
        ]
    }

    person_source = source_ctx.tables[0]
    assert (
        person_source.gen_concrete_create_stmt()
        == """
CREATE TABLE person_source (
    ssn VARCHAR(100),
    empid VARCHAR(100),
    name VARCHAR(100),
    hdate VARCHAR(100),
    phone VARCHAR(100),
    email VARCHAR(100),
    dept VARCHAR(100),
    manager VARCHAR(100)
)""".strip()
    )

    assert (
        person_source.gen_universal_create_stmt()
        == "CREATE TABLE person_source(ssn, empid, name, hdate, phone, email, dept, manager) AS SELECT universal.ssn, universal.empid, universal.name, universal.hdate, universal.phone, universal.email, universal.dept, universal.manager FROM universal"
    )


def test_context_direction(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    assert ctx.source.direction == "source"
    assert ctx.target.direction == "target"


def test_context_primary_keys(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    src_pks = ctx.source.primary_keys
    assert "person_source" in src_pks
    assert src_pks["person_source"] == ["ssn"]

    tgt_pks = ctx.target.primary_keys
    assert "person" in tgt_pks
    assert tgt_pks["person"] == ["ssn"]
    assert tgt_pks["personphone"] == ["ssn", "phone"]


def test_context_constraint_nodes(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    src = ctx.source
    assert len(src.functional_dependencies) == 3
    assert all(
        isinstance(fd, FunctionalDependencyNode) for fd in src.functional_dependencies
    )
    assert len(src.multivalued_dependencies) == 2
    assert all(
        isinstance(m, MultivaluedDependencyNode) for m in src.multivalued_dependencies
    )

    tgt = ctx.target
    assert len(tgt.inclusion_equivalences) == 5
    assert len(tgt.inclusion_subsumptions) == 3
