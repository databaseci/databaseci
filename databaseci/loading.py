import csv
from io import StringIO as sio

from .curs import DictRow
from .rows import Rows


def rows_from_text(t, format="csv", **kwargs):

    r = csv.reader(sio(t.strip()))

    column_names = next(r)

    index = {k: i for i, k in enumerate(column_names)}

    rows = Rows(DictRow(index=index, items=row) for row in r)

    rows.column_info = {k: "text" for k in column_names}

    return rows
