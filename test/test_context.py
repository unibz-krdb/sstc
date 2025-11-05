import os

from fixtures import example_1_dir as example_1_dir

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
