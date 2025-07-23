from typing import Self

from .TransducerContext import (
    Context,
    InsertTable,
    JoinTable,
    SourceTarget,
)
import os


class Generator:
    context: Context
    insert_tables: dict[str, InsertTable]
    join_tables: dict[str, JoinTable]

    def __init__(self, context: Context) -> None:
        self.context = context
        self.insert_tables = {}
        self.delete_tables = {}
        self.join_tables = {}

        for tablename, table in context.source.tables.items():
            self.schema = table.schema
            self.insert_tables[tablename] = InsertTable(source=table)
            self.join_tables[tablename] = JoinTable(
                create_table=table, context=context.source
            )

        for tablename, table in context.target.tables.items():
            self.insert_tables[tablename] = InsertTable(source=table)
            self.join_tables[tablename] = JoinTable(
                create_table=table, context=context.target
            )

    @classmethod
    def from_dir(cls, path: str) -> Self:
        context = Context.from_dir(path)
        return cls(context)

    def table_definitions(self, source_target: SourceTarget) -> str:
        result = "/****************************/\n"
        match source_target:
            case SourceTarget.SOURCE:
                result += "/* SOURCE TABLE DEFINITIONS */\n"
                context = self.context.source
            case SourceTarget.TARGET:
                result += "/* TARGET TABLE DEFINITIONS */\n"
                context = self.context.target
        result += "/****************************/\n\n"

        for tablename in context.ordering:
            table = context.tables[tablename]
            name_len = len("-- " + table.schema + "." + tablename + " --")
            result += "-" * name_len + "\n"
            result += f"-- {table.schema}.{tablename} --\n"
            result += "-" * name_len + "\n\n"
            result += f"-- create\n"
            result += table.create_stmt() + "\n\n"
            result += f"-- insert table\n"
            result += self.insert_tables[tablename].create_sql() + "\n\n"
            result += f"-- insert join table\n"
            result += self.join_tables[tablename].create_insert_sql() + "\n\n"
            constraints = context.constraints.get(tablename, [])
            if len(constraints) == 0:
                result += f"-- no constraints\n\n"
            for i, constraint in enumerate(constraints):
                result += f"-- constraint {i+1} of {len(constraints)}\n"
                result += constraint.generate_function() + "\n"
                result += constraint.generate_trigger() + "\n\n"

        return result

    def table_functions_and_triggers(self) -> str:
        result = "/******************************/\n"
        result += "/* TABLE FUNCTIONS & TRIGGERS */\n"
        result += "/******************************/\n\n"

        result += f"-- loop prevention mechanism\n"
        result += f"CREATE TABLE {self.schema}._LOOP (loop_start INT NOT NULL );\n\n"

        tablenames = self.context.source.ordering + self.context.target.ordering

        for tablename in tablenames:
            table = self.insert_tables[tablename]
            name_len = len("-- " + table.schema + "." + tablename + " --")
            result += "-" * name_len + "\n"
            result += f"-- {table.schema}.{tablename} --\n"
            result += "-" * name_len + "\n\n"
            result += f"-- insert function\n"
            result += table.generate_function() + "\n\n"
            result += f"-- insert trigger\n"
            result += table.generate_trigger() + "\n\n"

            table = self.join_tables[tablename]
            result += f"-- insert join function\n"
            result += table.generate_insert_function() + "\n\n"
            result += f"-- insert join trigger\n"
            result += table.generate_insert_trigger() + "\n"

        result += "\n"

        return result

    def source_target_functions_and_triggers(self) -> str:
        result = "/**************************************/\n"
        result += "/* SOURCE/TARGET FUNCTIONS & TRIGGERS */\n"
        result += "/**************************************/\n\n"

        result += "------------\n"
        result += "-- insert --\n"
        result += "------------\n\n"

        result += "-- S -> T\n"
        result += self.context.generate_target_insert() + "\n\n"
        for table in self.context.target.tables.keys():
            result += (
                self.context.generate_target_insert_trigger(
                    tablename=table + "_INSERT_JOIN"
                ) + "\n\n"
            )

        result += "-- T -> S\n"
        result += self.context.generate_source_insert() + "\n\n"
        for table in self.context.source.tables.keys():
            result += (
                self.context.generate_source_insert_trigger(
                    tablename=table + "_INSERT_JOIN"
                ) + "\n\n"
            )

        return result

    def generate(self) -> str:
        transducer = """DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

"""
        transducer += self.table_definitions(source_target=SourceTarget.SOURCE)

        transducer += self.table_definitions(source_target=SourceTarget.TARGET)

        transducer += self.table_functions_and_triggers()

        transducer += self.source_target_functions_and_triggers()

        return transducer

    def generate_to_path(self, path: str):
        with open(path, "w") as f:
            f.write(self.generate())
