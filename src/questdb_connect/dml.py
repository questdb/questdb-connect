from __future__ import annotations

from typing import Any, Optional, Union, Sequence

from sqlalchemy.sql import Select as StandardSelect
from sqlalchemy.sql import ClauseElement
from sqlalchemy import select as sa_select
from sqlalchemy.sql.visitors import Visitable


class SampleByClause(ClauseElement):
    """Represents the QuestDB SAMPLE BY clause."""

    __visit_name__ = "sample_by"
    stringify_dialect = "questdb"

    def __init__(
            self,
            value: Union[int, float],
            unit: Optional[str] = None
    ):
        self.value = value
        self.unit = unit.lower() if unit else None

    def __str__(self) -> str:
        if self.unit:
            return f"SAMPLE BY {self.value}{self.unit}"
        return f"SAMPLE BY {self.value}"

    def get_children(self, **kwargs: Any) -> Sequence[Visitable]:
        return []


class QDBSelect(StandardSelect):
    """QuestDB-specific implementation of SELECT.

    Adds methods for QuestDB-specific syntaxes such as SAMPLE BY.

    The :class:`_questdb.QDBSelect` object is created using the
    :func:`sqlalchemy.dialects.questdb.select` function.
    """

    stringify_dialect = "questdb"
    _sample_by_clause: Optional[SampleByClause] = None

    def get_children(self, **kwargs: Any) -> Sequence[Visitable]:
        children = super().get_children(**kwargs)
        if self._sample_by_clause is not None:
            children = children + [self._sample_by_clause]
        return children

    def sample_by(
            self,
            value: Union[int, float],
            unit: Optional[str] = None
    ) -> QDBSelect:
        """Add a SAMPLE BY clause to the select statement.

        The SAMPLE BY clause allows time-based sampling of data.

        :param value:
            For time-based sampling: the time interval


        :param unit:
            Time unit for sampling:
            - 's': seconds
            - 'm': minutes
            - 'h': hours
            - 'd': days

        Example time-based sampling::

            select([table.c.value]).sample_by(1, 'h')  # sample every hour
            select([table.c.value]).sample_by(30, 'm')  # sample every 30 minutes

        """
        # Create a copy of our object with _generative
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()

        # Set the sample by clause
        s._sample_by_clause = SampleByClause(value, unit)
        return s


def select(*entities: Any, **kwargs: Any) -> QDBSelect:
    """Construct a QuestDB-specific variant :class:`_questdb.Select` construct.

    .. container:: inherited_member

        The :func:`sqlalchemy.dialects.questdb.select` function creates
        a :class:`sqlalchemy.dialects.questdb.Select`. This class is based
        on the dialect-agnostic :class:`_sql.Select` construct which may
        be constructed using the :func:`_sql.select` function in
        SQLAlchemy Core.

    The :class:`_questdb.Select` construct includes additional method
    :meth:`_questdb.Select.sample_by` for QuestDB's SAMPLE BY clause.
    """
    stmt = sa_select(*entities, **kwargs)
    # Convert the SQLAlchemy Select into our QDBSelect
    qdbs = QDBSelect.__new__(QDBSelect)
    qdbs.__dict__ = stmt.__dict__.copy()
    return qdbs