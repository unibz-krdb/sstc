from dataclasses import dataclass
from typing import Literal

from .db_context import DbContext
from .constraint import Constraint
from .table import Table


@dataclass
class JoinTable:
    create_table: Table
    context: DbContext

    def __init__(self, create_table: Table, context: DbContext) -> None:
        self.create_table = create_table
        self.context = context
        self.insert_tablename = create_table.name + "_INSERT_JOIN"
        self.delete_tablename = create_table.name + "_DELETE_JOIN"

    def _create_sql(self, tablename: str) -> str:
        sql = f"CREATE TABLE {self.create_table.schema}.{tablename} AS\n"
        sql += f"SELECT * FROM {self.create_table.schema}.{self.create_table.name}\n"
        sql += "WHERE 1<>1;"
        return sql

    def create_insert_sql(self) -> str:
        return self._create_sql(self.insert_tablename)

    def create_delete_sql(self) -> str:
        return self._create_sql(self.delete_tablename)

    def generate_insert_function(self) -> str:
        return self._generate_function(
            self.insert_tablename, insert_delete=Constraint.InsertDelete.INSERT
        ).strip()

    def generate_delete_function(self) -> str:
        return self._generate_function(
            self.delete_tablename, insert_delete=Constraint.InsertDelete.DELETE
        )

    def _generate_function(
        self, tablename: str, insert_delete: Constraint.InsertDelete
    ) -> str:
        ordering = self.context.ordering

        if insert_delete == Constraint.InsertDelete.INSERT:
            suffix = "_INSERT"
            ordering = list(reversed(ordering))
        else:
            suffix = "_DELETE"

        # Function Header
        sql = f"""CREATE OR REPLACE FUNCTION {self.create_table.schema}.{tablename}_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function {self.create_table.schema}.{tablename}_FN called';
"""

        # Create temporary table
        sql += self.context.create_temp_table("temp_table")

        to_sql = self.create_table.mapping_sql(
            select_preamble="SELECT",
            primary_suffix=suffix
        )
        sql += f"\nINSERT INTO temp_table ({to_sql});\n"

        # Inserts

        for i, tablename in enumerate(ordering):
            table = self.context.tables[tablename]
            partner_sql = table.from_full_join(tablename="temp_table")
            sql += f"\nINSERT INTO {self.create_table.schema}.{table.name}{suffix}_JOIN ({partner_sql});"

            if i == len(ordering) - 2:
                sql += f"\nINSERT INTO {self.create_table.schema}._LOOP VALUES (1);"

        # Conclude

        sql += """\n
DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;
        """
        return sql

    def generate_trigger(
        self, tablename: str, _type: Literal["INSERT"] | Literal["DELETE"]
    ) -> str:
        sql = f"""CREATE TRIGGER {tablename}_trigger
AFTER INSERT ON {self.create_table.schema}.{self.create_table.name}_{_type}
FOR EACH ROW
EXECUTE FUNCTION {self.create_table.schema}.{tablename}_fn();
        """
        return sql

    def generate_insert_trigger(self) -> str:
        return self.generate_trigger(self.insert_tablename, "INSERT")

    def generate_delete_trigger(self) -> str:
        return self.generate_trigger(self.delete_tablename, "DELETE")
