from dataclasses import dataclass

from .enums import SourceTarget
from .table import Table


@dataclass
class InsertTable:
    source: Table
    schema: str
    table: str

    def __init__(self, source: Table) -> None:
        self.source = source
        self.schema = source.schema
        self.table = source.name + "_INSERT"

    def create_sql(self) -> str:
        sql = f"CREATE TABLE {self.source.schema}.{self.table} AS\n"
        sql += f"SELECT * FROM {self.source.schema}.{self.source.name}\n"
        sql += "WHERE 1<>1;"
        return sql

    def generate_function(self, source_target: SourceTarget) -> str:
        match source_target:
            case SourceTarget.SOURCE:
                loop_val = -1
            case SourceTarget.TARGET:
                loop_val = 1
        function_name = f"{self.source.schema}.{self.table}_fn"
        attributestr = ", ".join(f"new.{attr.name}" for attr in self.source.attributes)
        sql = f"""CREATE OR REPLACE FUNCTION {function_name}()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function {function_name} called';
   IF EXISTS (SELECT * FROM {self.source.schema}._loop where loop_start = {loop_val}) THEN
      RETURN NULL;
   ELSE
      INSERT INTO {self.source.schema}.{self.table} VALUES({attributestr});
      RETURN NEW;
   END IF;
END;  $$;
"""

        return sql.strip()

    def generate_trigger(self) -> str:
        sql = f"""CREATE TRIGGER {self.source.schema}_{self.table}_trigger
AFTER INSERT ON {self.source.schema}.{self.source.name}
FOR EACH ROW
EXECUTE FUNCTION {self.source.schema}.{self.table}_fn();
"""
        return sql.strip()
