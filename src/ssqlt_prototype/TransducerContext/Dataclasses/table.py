from dataclasses import dataclass
from typing import Literal
from mo_sql_parsing import parse
from jinja2 import Template


@dataclass(eq=True, frozen=True)
class Attr:
    name: str
    _type: str
    nullable: bool

    @classmethod
    def from_dict(cls, col: dict):
        attr_name = col["name"]
        attr_type = list(col["type"])[0]
        if col["type"][attr_type] != {}:
            attr_type = attr_type + "(" + str(col["type"][attr_type]) + ")"
        attr_nullable = col.get("nullable", True)
        return cls(
            name=attr_name.lower(), _type=attr_type.upper(), nullable=attr_nullable
        )


@dataclass
class PK:
    columns: list[str]

    @classmethod
    def from_dict(cls, pk: dict):
        if isinstance(pk["columns"], list):
            columns = [col.lower() for col in pk["columns"]]
        elif isinstance(pk["columns"], str):
            columns = [pk["columns"].lower()]
        else:
            raise ValueError("Primary key columns must be a list or a string")
        return cls(columns=columns)


@dataclass
class FK:
    columns: list[str]
    ref_tablename: str
    ref_tableschema: str
    ref_columns: list[str]

    @classmethod
    def from_dict(cls, fk: dict):

        cols = fk["columns"]
        if isinstance(cols, list):
            columns = [col.lower() for col in cols]
        elif isinstance(cols, str):
            columns = [cols.lower()]
        else:
            raise ValueError("Foreign key columns must be a list or a string")

        references = fk["references"]
        (ref_tableschema, ref_tablename) = references["table"].lower().split(".")

        ref_cols = references["columns"]
        if isinstance(ref_cols, list):
            ref_columns = [col.lower() for col in ref_cols]
        elif isinstance(ref_cols, str):
            ref_columns = [ref_cols.lower()]
        else:
            raise ValueError("Referenced columns must be a list or a string")

        return cls(
            columns=columns,
            ref_tablename=ref_tablename,
            ref_tableschema=ref_tableschema,
            ref_columns=ref_columns,
        )


@dataclass
class Table:
    schema: str
    name: str
    attributes: list[Attr]
    pkey: list[PK]
    fkey: list[FK]
    mapping: Template

    def create_stmt(self) -> str:
        attrs = ",\n".join(f"\t{attr.name} {attr._type}" for attr in self.attributes)

        sql = f"CREATE TABLE {self.schema}.{self.name} ("
        sql += f"\n{attrs}"

        if len(self.pkey) > 0:
            pkeys = ", ".join(f"{col}" for pk in self.pkey for col in pk.columns)
            sql += f",\n\tPRIMARY KEY ({pkeys})" if pkeys else ""

        if len(self.fkey) > 0:
            fkeys = ", ".join(
                f"FOREIGN KEY ({', '.join(fk.columns)}) REFERENCES {fk.ref_tableschema}.{fk.ref_tablename} ({', '.join(fk.ref_columns)})"
                for fk in self.fkey
            )
            sql += f",\n\t{fkeys}" if fkeys else ""

        sql += "\n);"

        return sql.strip()

    @classmethod
    def from_create_path(cls, create_path: str, mapping_path: str):
        with open(create_path, "r") as f:
            create_sql = f.read().strip()
        with open(mapping_path, "r") as f:
            mapping_sql = f.read().strip()
        return cls.from_create_stmt(create_sql, mapping_sql)

    @classmethod
    def from_create_stmt(cls, sql: str, mapping_sql: str) -> "Table":

        parsed: dict = parse(sql)
        if "create table" not in parsed:
            raise ValueError("Invalid CREATE TABLE statement")

        statement = parsed["create table"]

        try:
            (schema, tablename) = statement["name"].lower().split(".")
        except ValueError:
            raise ValueError(
                "CREATE TABLE statement must include schema and table name"
            )

        attrs = list(map(Attr.from_dict, statement["columns"]))

        constraints = statement.get("constraint", [])
        if isinstance(constraints, dict):
            constraints = [constraints]
        pkeys = []
        fkeys = []
        for constraint in constraints:
            if "primary_key" in constraint:
                pkeys.append(PK.from_dict(constraint["primary_key"]))
            elif "foreign_key" in constraint:
                fkeys.append(FK.from_dict(constraint["foreign_key"]))
            else:
                raise ValueError("Unknown constraint type in CREATE TABLE statement")

        return cls(
            schema=schema,
            name=tablename,
            attributes=list(attrs),
            pkey=pkeys,
            fkey=fkeys,
            mapping=Template(mapping_sql),
        )

    def mapping_sql(
        self,
        select_preamble: (
            Literal["SELECT"] | Literal["SELECT DISTINCT"] | Literal[""]
        ) = "",
        custom_attributes: list[Attr] | None = None,
        primary_suffix: str = "",
        secondary_suffix: str = "",
        where: bool = False
    ) -> str:
        """
        Returns the SQL for the mapping of this table.
        """
        if custom_attributes is None:
            custom_attributes = self.attributes

        attr_str = ", ".join(
            f"{attr.name}" for attr in custom_attributes
        )

        if not where:
            return self.mapping.render(
                select_preamble=select_preamble, attributes=attr_str, primary_suffix=primary_suffix, secondary_suffix=secondary_suffix, where=""
            )
        else:
            return self.mapping.render(
                select_preamble=select_preamble, attributes=attr_str, primary_suffix=primary_suffix, secondary_suffix=secondary_suffix
            )

    def from_full_join(self, tablename: str, schema: str | None = None):
        if schema is not None:
            from_tablename = schema + "."
        else:
            from_tablename = ""
        from_tablename += tablename
        return f"SELECT {', '.join(attr.name for attr in self.attributes)} FROM {from_tablename}"
