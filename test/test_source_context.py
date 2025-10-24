import os

from fixtures import example_1_dir as example_1_dir

from sstc import SourceContext, TargetContext


def test_source_context_from_file(example_1_dir: str):
    context = SourceContext.from_file(
        schema_path=os.path.join(example_1_dir, "source_schema.json"),
        constraints_path=os.path.join(example_1_dir, "source_constraints.txt"),
    )
    assert isinstance(context, SourceContext)
    assert len(context.tables) == 1
    assert context.schema.to_dict() == {
        "person_ura": [
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

    context = TargetContext.from_file(
        os.path.join(example_1_dir, "target.txt"), source_context=context
    )
    assert isinstance(context, TargetContext)
    assert len(context.tables) == 8
