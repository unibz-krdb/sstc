from rapt2.treebrd.node import (
    FunctionalDependencyNode,
    MultivaluedDependencyNode,
)


def test_context(example_1_ctx):
    transducer_ctx = example_1_ctx

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


def test_context_direction(example_1_ctx):
    assert example_1_ctx.source.direction == "source"
    assert example_1_ctx.target.direction == "target"


def test_context_primary_keys(example_1_ctx):
    src_pks = example_1_ctx.source.primary_keys
    assert "person_source" in src_pks
    assert src_pks["person_source"] == ["ssn"]

    tgt_pks = example_1_ctx.target.primary_keys
    assert "person" in tgt_pks
    assert tgt_pks["person"] == ["ssn"]
    assert tgt_pks["personphone"] == ["ssn", "phone"]


def test_context_constraint_nodes(example_1_ctx):
    src = example_1_ctx.source
    assert len(src.functional_dependencies) == 3
    assert all(
        isinstance(fd, FunctionalDependencyNode) for fd in src.functional_dependencies
    )
    assert len(src.multivalued_dependencies) == 2
    assert all(
        isinstance(m, MultivaluedDependencyNode) for m in src.multivalued_dependencies
    )

    tgt = example_1_ctx.target
    assert len(tgt.inclusion_equivalences) == 5
    assert len(tgt.inclusion_subsumptions) == 3


def test_example2_context_primary_keys(example_2_ctx):
    """Example2 composite PK (ssn, phone, email) correctly parsed."""
    assert example_2_ctx.source.primary_keys["person_source"] == [
        "ssn",
        "phone",
        "email",
    ]


def test_example2_context_nullability(example_2_ctx):
    """Example2 mixed nullability: 4 mandatory, 4 nullable."""
    schema = example_2_ctx.source.universal_schema
    mandatory = [a.name for a in schema if not a.is_nullable]
    nullable = [a.name for a in schema if a.is_nullable]
    assert set(mandatory) == {"ssn", "name", "phone", "email"}
    assert set(nullable) == {"empid", "hdate", "dept", "manager"}
