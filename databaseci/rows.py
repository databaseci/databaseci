from .curs import DictRow
from .formatting import format_table_of_dicts
from .typeguess import guess_sql_type_of_values


class Rows(list):
    def __init__(self, *args, **kwargs):
        self.paging = None
        self.column_info = None

        super().__init__(*args, **kwargs)

    def __getitem__(self, x):
        if isinstance(x, (int, slice)):
            return super().__getitem__(x)

        if x not in self.column_info:
            raise ValueError(f"no such column: {x}")

        return [_[x] for _ in self]

    def as_dicts(self):
        return [dict(_) for _ in self]

    def __str__(self):
        return format_table_of_dicts(self.as_dicts())

    @property
    def hierarchical(self):
        return format_table_of_dicts(self.as_dicts(), hierarchical=True)

    @property
    def guessed_sql_columns(self):
        return {k: guess_sql_type_of_values(self[k]) for k in self.column_info}

    @property
    def from_dicts(list_of_dicts):
        first = list_of_dicts[0]
        column_names = list(first.keys())

        index = {k: i for i, k in enumerate(column_names)}

        rows = Rows(DictRow(index=index, items=list(d.values())) for d in list_of_dicts)

        rows.column_info = {k: "text" for k in column_names}

        return rows
