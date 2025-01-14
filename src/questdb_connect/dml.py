from __future__ import annotations

from datetime import datetime, date
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

from sqlalchemy import select as sa_select
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql import Select as StandardSelect

if TYPE_CHECKING:
    from sqlalchemy.sql.visitors import Visitable


class SampleByClause(ClauseElement):
    """Represents the QuestDB SAMPLE BY clause."""

    __visit_name__ = "sample_by"
    stringify_dialect = "questdb"

    def __init__(
            self,
            value: Union[int, float],
            unit: Optional[str] = None,
            fill: Optional[Union[str, float]] = None,
            align_to: str = "CALENDAR",  # default per docs
            timezone: Optional[str] = None,
            offset: Optional[str] = None,
            from_timestamp: Optional[Union[datetime, date]] = None,
            to_timestamp: Optional[Union[datetime, date]] = None
    ):
        self.value = value
        self.unit = unit.lower() if unit else None
        self.fill = fill
        self.align_to = align_to.upper()
        self.timezone = timezone
        self.offset = offset
        self.from_timestamp = from_timestamp
        self.to_timestamp = to_timestamp

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
            children = [*children, self._sample_by_clause]
        return children

    def sample_by(
            self,
            value: Union[int, float],
            unit: Optional[str] = None,
            fill: Optional[Union[str, float]] = None,
            align_to: str = "CALENDAR",
            timezone: Optional[str] = None,
            offset: Optional[str] = None,
            from_timestamp: Optional[Union[datetime, date]] = None,
            to_timestamp: Optional[Union[datetime, date]] = None,
    ) -> QDBSelect:
        """Add a SAMPLE BY clause.

        :param value: time interval value
        :param unit: 's' for seconds, 'm' for minutes, 'h' for hours, etc.
        :param fill: fill strategy - NONE, NULL, PREV, LINEAR, or constant value
        :param align_to: CALENDAR or FIRST OBSERVATION
        :param timezone: Optional timezone for calendar alignment
        :param offset: Optional offset in format '+/-HH:mm'
        :param from_timestamp: Optional start timestamp for the sample
        :param to_timestamp: Optional end timestamp for the sample
        """

        # Create a copy of our object with _generative
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()

        # Set the sample by clause
        s._sample_by_clause = SampleByClause(
            value, unit, fill, align_to, timezone, offset, from_timestamp, to_timestamp
        )
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