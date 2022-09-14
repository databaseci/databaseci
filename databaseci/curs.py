from collections import OrderedDict
from collections.abc import Mapping
from pprint import pformat
from typing import Iterable

from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor

from .constants import type_codes


class DictCursorBase(_cursor):
    """Base class for all dict-like cursors."""

    def __init__(self, *args, **kwargs):
        if "row_factory" in kwargs:
            row_factory = kwargs["row_factory"]
            del kwargs["row_factory"]
        else:
            raise NotImplementedError(
                "DictCursorBase can't be instantiated without a row factory."
            )
        super().__init__(*args, **kwargs)
        self._query_executed = False
        self._prefetch = False
        self.row_factory = row_factory

    def fetchone(self):
        if self._prefetch:
            res = super().fetchone()
        if self._query_executed:
            self._build_index()
        if not self._prefetch:
            res = super().fetchone()
        return res

    def fetchmany(self, size=None):
        if self._prefetch:
            res = super().fetchmany(size)
        if self._query_executed:
            self._build_index()
        if not self._prefetch:
            res = super().fetchmany(size)
        return res

    def fetchall(self):
        if self._prefetch:
            # breakpoint()
            res = super().fetchall()
        if self._query_executed:
            self._build_index()
        if not self._prefetch:
            res = super().fetchall()
        return res

    def __iter__(self):
        try:
            if self._prefetch:
                res = super().__iter__()
                first = next(res)
            if self._query_executed:
                self._build_index()
            if not self._prefetch:
                res = super().__iter__()
                first = next(res)

            yield first
            while True:
                yield next(res)
        except StopIteration:
            return


class DictConnection(_connection):
    """A connection that uses `DictCursor` automatically."""

    def cursor(self, *args, **kwargs):
        kwargs.setdefault("cursor_factory", self.cursor_factory or DictCursor)
        return super().cursor(*args, **kwargs)


class DictCursor(DictCursorBase):
    """A cursor that keeps a list of column name -> index mappings__.
    .. __: https://docs.python.org/glossary.html#term-mapping
    """

    def __init__(self, *args, **kwargs):
        kwargs["row_factory"] = DictRow
        super().__init__(*args, **kwargs)
        self._prefetch = True

    def execute(self, query, vars=None):
        self.index = OrderedDict()
        self._query_executed = True
        return super().execute(query, vars)

    def callproc(self, procname, vars=None):
        self.index = OrderedDict()
        self._query_executed = True
        return super().callproc(procname, vars)

    def _build_index(self):
        if self._query_executed and self.description:
            for i in range(len(self.description)):
                self.index[self.description[i][0]] = i
            self._query_executed = False


class DictRow(list):
    """A row object that allow by-column-name access to data."""

    __slots__ = ("_index",)

    def __init__(self, cursor=None, *, index=None, items=None):
        if cursor:
            self._index = cursor.index

            self[:] = [None] * len(cursor.description)
        elif index:
            self._index = index

        if items:
            shortage = len(index) - len(items)

            if shortage:
                _items = list(items) + ([None] * shortage)
                super().__init__(_items)
            else:
                super().__init__(items)

    def __getitem__(self, x):
        if not isinstance(x, (int, slice)):
            x = self._index[x]
        return super().__getitem__(x)

    def __setitem__(self, x, v):
        if not isinstance(x, (int, slice)):
            x = self._index[x]
        super().__setitem__(x, v)

    def items(self):
        g = super().__getitem__
        return ((n, g(self._index[n])) for n in self._index)

    def keys(self):
        return iter(self._index)

    def values(self):
        g = super().__getitem__
        return (g(self._index[n]) for n in self._index)

    def get(self, x, default=None):
        try:
            return self[x]
        except LookupError:
            return default

    def __getattribute__(self, __name: str):
        try:
            i = object.__getattribute__(self, "_index")[__name]
            return super().__getitem__(i)
        except LookupError:
            pass

        return object.__getattribute__(self, __name)

    def as_dict(self):
        return dict(self.items())

    def __contains__(self, x):
        return x in self._index

    def __reduce__(self):
        # this is apparently useless, but it fixes #1073
        return super().__reduce__()

    def __getstate__(self):
        return self[:], self._index.copy()

    def __setstate__(self, data):
        self[:] = data[0]
        self._index = data[1]

    def __eq__(self, other):
        if isinstance(other, DictRow):
            return self.as_dict() == other.as_dict()

        if isinstance(other, Mapping):
            return self.as_dict() == other

        else:
            return self == other

    def __str__(self):
        return pformat(self.as_dict(), width=-1)
