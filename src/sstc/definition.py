"""JSON-deserializable schema types for the universal relation.

Provides dataclass definitions that mirror the structure of the universal
schema JSON files, using DataClassJsonMixin for automatic serialization.
"""

from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin


@dataclass
class AttributeSchema(DataClassJsonMixin):
    """A single column's metadata from the universal schema JSON."""

    name: str
    data_type: str
    is_nullable: bool
