import os
from fixtures import resource_dir as resource_dir, input_dir as input_dir

from ssqlt_prototype import Constraint


def test_from_file(input_dir):
    file_path = os.path.join(
        input_dir, "source", "constraints", "transducer._empdep.fd.1.before.insert.sql"
    )
    constraint = Constraint.from_file(file_path)
    assert constraint.schema == "transducer"
    assert constraint.table == "_empdep"
    assert constraint.type_ == "fd"
    assert constraint.index == 1
    assert constraint.insert_delete == Constraint.InsertDelete.INSERT
    assert constraint.before_after == Constraint.BeforeAfter.BEFORE
    with open(file_path, "r") as f:
        assert constraint.generate_function() == f.read().strip()
