from fixtures import example_1_dir as example_1_dir
from sstc import SourceContext, TargetContext
import os

def test_source_context_from_file(example_1_dir: str):
    context = SourceContext.from_file(os.path.join(example_1_dir, "source.txt"))
    assert isinstance(context, SourceContext)
    assert len(context.relations) == 1
    assert len(context.dependencies) == 7
    assert context.schema.to_dict() == { "person_ura": ["ssn", "empid", "name", "hdate", "phone", "email", "dept", "manager"] }

    context = TargetContext.from_file(os.path.join(example_1_dir, "target.txt"), source_context=context)
    assert isinstance(context, TargetContext)
    assert len(context.relations) == 8
    assert len(context.dependencies) == 16
