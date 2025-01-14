import abc

import sqlalchemy
from sqlalchemy.sql.base import elements

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
    def visit_sample_by(self, sample_by, **kw):
        """Compile a SAMPLE BY clause."""
        text = ""

        # Basic SAMPLE BY
        if sample_by.unit:
            text = f"SAMPLE BY {sample_by.value}{sample_by.unit}"
        else:
            text = f"SAMPLE BY {sample_by.value}"

        # Add FILL if specified
        if sample_by.fill is not None:
            if isinstance(sample_by.fill, str):
                text += f" FILL({sample_by.fill})"
            else:
                text += f" FILL({sample_by.fill:g})"

        # Add ALIGN TO clause
        text += f" ALIGN TO {sample_by.align_to}"

        # Add TIME ZONE if specified
        if sample_by.timezone:
            text += f" TIME ZONE '{sample_by.timezone}'"

        # Add WITH OFFSET if specified
        if sample_by.offset:
            text += f" WITH OFFSET '{sample_by.offset}'"

        return text

    def group_by_clause(self, select, **kw):
        """Customize GROUP BY to also render SAMPLE BY."""
        text = ""

        # Add SAMPLE BY first if present
        if select._sample_by_clause is not None:
            text += " " + self.process(select._sample_by_clause, **kw)

        # Use parent's GROUP BY implementation
        group_by_text = super().group_by_clause(select, **kw)
        if group_by_text:
            text += group_by_text

        return text

    def visit_select(self, select, **kw):
        """Add SAMPLE BY support to the standard SELECT compilation."""

        # If we have SAMPLE BY but no GROUP BY,
        # add a dummy GROUP BY clause to trigger the rendering
        if (
                select._sample_by_clause is not None
                and not select._group_by_clauses
        ):
            select = select._clone()
            select._group_by_clauses = [elements.TextClause("")]

        text = super().visit_select(select, **kw)
        return text

    def _is_safe_for_fast_insert_values_helper(self):
        return True

    def visit_textclause(self, textclause, add_to_result_map=None, **kw):
        textclause.text = remove_public_schema(textclause.text)
        return super().visit_textclause(textclause, add_to_result_map, **kw)
