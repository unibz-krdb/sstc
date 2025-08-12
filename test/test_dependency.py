from ssqlt_prototype import Dependency


def test_mvd():
    constraint_str = "mvd[Transducer.Person](ssn, phone)"
    constraint = Dependency.parse(constraint_str)
    assert constraint.type_ == Dependency.Type.MVD
    assert constraint.schema == "transducer"
    assert constraint.table == "person"
    assert constraint.left == "ssn"
    assert constraint.right == "phone"
    assert constraint.conditions == []


def test_fd():
    constraint_str = "fd[transducer.person](empid,hdate)(empid,hdate)"
    constraint = Dependency.parse(constraint_str)
    assert constraint.type_ == Dependency.Type.FD
    assert constraint.schema == "transducer"
    assert constraint.table == "person"
    assert constraint.left == "empid"
    assert constraint.right == "hdate"
    assert constraint.conditions == ["empid", "hdate"]


def test_ind():
    constraint_str = "ind[transducer.Person](manager,empid)"
    constraint = Dependency.parse(constraint_str)
    assert constraint.type_ == Dependency.Type.IND
    assert constraint.schema == "transducer"
    assert constraint.table == "person"
    assert constraint.left == "manager"
    assert constraint.right == "empid"
    assert constraint.conditions == []
