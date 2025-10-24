from abc import ABC, abstractclassmethod, abstractmethod, abstractproperty
from dataclasses import dataclass

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
class SourceDefinition(Definition):

    schema: dict

    @property
    def name(self) -> str:
        if "name" not in self.schema:
            raise ValueError("Schema must have a name")
        return self.schema["name"]

    @property
    def attributes(self) -> list[str]:
        if "attributes" not in self.schema:
            raise ValueError("Schema must have attributes")
        return [attribute["name"] for attribute in self.schema["attributes"]]
