from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin
from rapt2.treebrd.node import AssignNode


@dataclass
class Definition(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the definition."""
        raise NotImplementedError

    @property
    @abstractmethod
    def attributes(self) -> list[str]:
        """Return the list of attribute names for the definition."""
        raise NotImplementedError


@dataclass
class TargetDefinition(Definition):

    node: AssignNode

    @property
    def name(self) -> str:
        if self.node.name is None:
            raise ValueError("AssignNode must have a name")
        return self.node.name

    @property
    def attributes(self) -> list[str]:
        return self.node.attributes.names


@dataclass
class AttributeSchema(DataClassJsonMixin):
    name: str
    data_type: str
    is_nullable: bool


@dataclass
class TableSchema(DataClassJsonMixin):
    name: str
    attributes: list[AttributeSchema]


@dataclass
class SourceDefinition(Definition):

    schema: TableSchema

    @property
    def name(self) -> str:
        return self.schema.name

    @property
    def attributes(self) -> list[str]:
        return [attribute.name for attribute in self.schema.attributes]
