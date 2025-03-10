import abc

import sqlalchemy

from .common import quote_identifier, remove_public_schema
from .types import QDBTypeMixin


class QDBDDLCompiler(sqlalchemy.sql.compiler.DDLCompiler, abc.ABC):
    def visit_create_schema(self, create, **kw):
        raise Exception("QuestDB does not support SCHEMAS, there is only 'public'")

    def visit_drop_schema(self, drop, **kw):
        raise Exception("QuestDB does not support SCHEMAS, there is only 'public'")

    def visit_create_table(self, create, **kw):
        table = create.element
        create_table = f"CREATE TABLE {quote_identifier(table.fullname)} ("
        create_table += ", ".join(
            [self.get_column_specification(c.element) for c in create.columns]
        )
        return create_table + ") " + table.engine.get_table_suffix()

    def get_column_specification(self, column: sqlalchemy.Column, **_):
        if not isinstance(column.type, QDBTypeMixin):
            raise sqlalchemy.exc.ArgumentError(
                "Column type is not a valid QuestDB type"
            )
        return column.type.column_spec(column.name)


class QDBSQLCompiler(sqlalchemy.sql.compiler.SQLCompiler, abc.ABC):
    # Maximum value for 64-bit signed integer (2^63 - 1)
    BIGINT_MAX = 9223372036854775807

    def _is_safe_for_fast_insert_values_helper(self):
        return True

    def visit_textclause(self, textclause, add_to_result_map=None, **kw):
        textclause.text = remove_public_schema(textclause.text)
        return super().visit_textclause(textclause, add_to_result_map, **kw)

    def limit_clause(self, select, **kw):
        """
        Generate QuestDB-style LIMIT clause from SQLAlchemy select statement.
        QuestDB supports arbitrary expressions in LIMIT clause.
        """
        text = ""
        limit = select._limit_clause
        offset = select._offset_clause

        if limit is None and offset is None:
            return text

        text += "\n LIMIT "

        # Handle cases based on presence of limit and offset
        if limit is not None and offset is not None:
            # Convert LIMIT x OFFSET y to LIMIT y,y+x
            lower_bound = self.process(offset, **kw)
            limit_val = self.process(limit, **kw)
            text += f"{lower_bound},{lower_bound} + {limit_val}"

        elif limit is not None:
            text += self.process(limit, **kw)

        elif offset is not None:
            # If only offset is specified, use max bigint as upper bound
            text += f"{self.process(offset, **kw)},{self.BIGINT_MAX}"

        return text
