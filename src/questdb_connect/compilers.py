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
    def visit_sample_by(self, sample_by, **kw):
        """Compile a SAMPLE BY clause."""
        if sample_by.unit:
            return f"SAMPLE BY {sample_by.value}{sample_by.unit}"
        return f"SAMPLE BY {sample_by.value}"

    def visit_select(self, select, **kw):
        """Add SAMPLE BY support to the standard SELECT compilation."""

        text = super().visit_select(select, **kw)

        # TODO: The exact positioning is a big funky, fix it
        if hasattr(select, '_sample_by_clause') and select._sample_by_clause is not None:
            # Add SAMPLE BY before ORDER BY and LIMIT
            sample_text = self.process(select._sample_by_clause, **kw)

            # Find positions of ORDER BY and LIMIT
            order_by_pos = text.find("ORDER BY")
            limit_pos = text.find("LIMIT")

            # Determine where to insert SAMPLE BY
            if order_by_pos >= 0:
                # Insert before ORDER BY
                text = text[:order_by_pos] + sample_text + " " + text[order_by_pos:]
            elif limit_pos >= 0:
                # Insert before LIMIT
                text = text[:limit_pos] + sample_text + " " + text[limit_pos:]
            else:
                # Append at the end
                text += " " + sample_text

        return text

    def _is_safe_for_fast_insert_values_helper(self):
        return True

    def visit_textclause(self, textclause, add_to_result_map=None, **kw):
        textclause.text = remove_public_schema(textclause.text)
        return super().visit_textclause(textclause, add_to_result_map, **kw)
