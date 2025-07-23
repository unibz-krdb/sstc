from dataclasses import dataclass
from typing import Self

from .context_dir import ContextDir
from .context_file_paths import ContextFilePaths
from .Dataclasses.db_context import DbContext


@dataclass
class Context:

    source: DbContext
    target: DbContext

    def __init__(self, context_files: ContextFilePaths) -> None:

        self.source = DbContext.from_files(
            create_paths=context_files.source_creates,
            constraint_paths=context_files.source_constraints,
            mapping_paths=context_files.source_mappings,
        )

        self.target = DbContext.from_files(
            create_paths=context_files.target_creates,
            constraint_paths=context_files.target_constraints,
            mapping_paths=context_files.target_mappings,
        )

    @classmethod
    def from_dir(cls, file_dir: str) -> Self:
        context_dirs = ContextDir.from_dir(file_dir)
        return cls(ContextFilePaths(context_dirs))

    def generate_target_insert_trigger(self, tablename: str) -> str:
        return f"""
CREATE TRIGGER target_insert_{tablename}_trigger
AFTER INSERT ON {self.target.schema}.{tablename}
FOR EACH ROW
EXECUTE FUNCTION {self.target.schema}.target_insert_fn();
""".strip()
        
    def generate_source_insert_trigger(self, tablename: str) -> str:
        return f"""
CREATE TRIGGER source_insert_{tablename}_trigger
AFTER INSERT ON {self.source.schema}.{tablename}
FOR EACH ROW
EXECUTE FUNCTION {self.source.schema}.source_insert_fn();
""".strip()

    def generate_target_insert(self):
        result = ""

        source_orderings = self.source.ordering
        target_orderings = self.target.ordering

        schema = "transducer"  # TODO Hardcoded
        temp_tablename = "temp_table_join"

        # Start
        result += f"""
CREATE OR REPLACE FUNCTION {schema}.target_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE
v_loop INT;
BEGIN

RAISE NOTICE 'Function {schema}.target_insert_fn called';

SELECT count(*) INTO v_loop from transducer._loop;


IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on _EMPDEP';
        """

        result += self.source.create_temp_table(temp_tablename)

        full_mapping_tablename = target_orderings[0]
        table = self.target.tables[full_mapping_tablename]
        full_mapping = table.mapping_sql(
            custom_attributes=self.target.all_attributes(),
            primary_suffix="_INSERT_JOIN",
            secondary_suffix="_INSERT_JOIN",
        )
        result += f"\n\nINSERT INTO {temp_tablename}("
        result += full_mapping
        result += "\n where "
        result += "\n AND ".join(
            map(lambda x: x.name + " IS NOT NULL", self.target.all_attributes())
        )
        result += "\n "
        result += ");"

        # Other inserts

        tablename = source_orderings[0]
        table = self.source.tables[tablename]

        mapping_str = table.from_full_join(tablename=temp_tablename)
        result += f"""
\nINSERT INTO {schema}.{tablename} ({mapping_str}) ON CONFLICT ({", ".join(table.pkey[0].columns)}) DO NOTHING;
INSERT INTO {schema}._loop VALUES (-1);
"""

        for tablename in source_orderings[1:]:
            table = self.source.tables[tablename]
            mapping_str = table.from_full_join(tablename=temp_tablename)
            result += f"INSERT INTO {schema}.{tablename} ({mapping_str}) ON CONFLICT ({', '.join(table.pkey[0].columns)}) DO NOTHING;"

        result += "\n"

        # DELETES

        for table in target_orderings[::-1]:
            result += f"\nDELETE FROM {schema}.{table}_INSERT;"

        result += "\n"

        for table in target_orderings[::-1]:
            result += f"\nDELETE FROM {schema}.{table}_INSERT_JOIN;"

        result += "\n"

        result += f"""
DELETE FROM {schema}._loop;
DELETE FROM {temp_tablename};
DROP TABLE {temp_tablename};

RETURN NEW;
END IF;
END;    $$;
"""

        return result.strip()

    def generate_source_insert(self):

        schema = "transducer"  # TODO Hardcoded

        result = f"""
CREATE OR REPLACE FUNCTION {schema}.source_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function {schema}.source_insert_fn called';
IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'But now is not the time to generate the query';
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on _EMPDEP';"""

        full_join_table = list(self.source.tables.values())[0]

        def get_insert(target: str):
            table = self.target.tables[target]
            mapping_str = "(" + full_join_table.mapping_sql(custom_attributes=table.attributes, primary_suffix="_INSERT_JOIN", secondary_suffix="_INSERT_JOIN", where=True) + ")"

            result = f"\n\tINSERT INTO {schema}.{target} "
            result += "" + mapping_str + ""
            result += " ON CONFLICT (" + ",".join(table.pkey[0].columns) + ") DO NOTHING;"
            return result

        for target in self.target.ordering:
            result += "\n" + get_insert(target)

        result += "\n"

        for tablename in self.source.ordering[::-1]:
            result += f"\n\tDELETE FROM {schema}.{tablename}_INSERT;"
        for tablename in self.source.ordering[::-1]:
            result += f"\n\tDELETE FROM {schema}.{tablename}_INSERT_JOIN;"

        result += f"\n\tDELETE FROM {schema}._loop;"

        result += """
RETURN NEW;
END IF;
END;  $$;
"""

        return result.strip()

    def generate_target_delete(self):
        return NotImplementedError("Target delete generation is not implemented yet.")
