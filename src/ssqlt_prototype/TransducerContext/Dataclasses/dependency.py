from dataclasses import dataclass
from enum import Enum

@dataclass
class Dependency:
    class Type(Enum):
        MVD = "mvd"
        FD = "fd"
        IND = "ind"

    schema: str
    table: str
    type_: Type
    left: str
    right: str
    conditions: list[str]

    @classmethod
    def parse(cls, string: str) -> "Dependency":
        """Parse a string representation of the constraint."""
        typestr = string[: string.find("[")]

        try:
            type_ = Dependency.Type(typestr)
        except ValueError:
            raise ValueError(f"Invalid constraint type: {typestr}")

        schema_name = string[string.find("[") + 1 : string.find("]")]
        schema, tablename = schema_name.split(".")
        left_right_part = string[string.find("(") + 1 : string.find(")")].split(",")
        left = left_right_part[0].strip()
        right = left_right_part[1].strip()

        conditions = []
        if string.find(")(") != -1:
            condition_part = string[string.find(")(") + 2 :].strip()
            if condition_part:
                conditions = [cond.strip() for cond in condition_part[:-1].split(",")]
        return cls(
            schema=schema.strip().lower(),
            table=tablename.strip().lower(),
            type_=type_,
            left=left.strip().lower(),
            right=right.strip().lower(),
            conditions=conditions,
        )
