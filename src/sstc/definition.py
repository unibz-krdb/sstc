from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin


@dataclass
class AttributeSchema(DataClassJsonMixin):
    name: str
    data_type: str
    is_nullable: bool
