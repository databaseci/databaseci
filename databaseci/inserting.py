from collections.abc import Mapping

from .rows import Rows

INSERT = """
    insert into
        {table} ({colspec})
    values
        %s
"""


INSERT_DEFAULT = """
    insert into
        {table}
    default values
"""


INSERT_UPSERT = """
    on conflict ({upsertkeyspec})
    do update set
        {upsertspec}
"""


INSERT_UPSERT = """
    on conflict ({upsertkeyspec})
    do update set
        {upsertspec}

"""


INSERT_UPSERT_DO_NOTHING = """
    on conflict ({upsertkeyspec})
    do nothing
"""


class Inserting:
    def insert(self, table, rows, upsert_on=None, returning=None):

        if isinstance(rows, Mapping):
            rows = [rows]

        if isinstance(upsert_on, str):
            upsert_on = [upsert_on]

        if not rows:
            raise ValueError("empty list of rows, nothing to upsert")

        if returning is None:
            returning = not len(rows) > 1

        if isinstance(rows, Rows):
            rows = rows.as_dicts()

        keys = list(rows[0].keys())

        if keys:
            colspec = ", ".join([f'"{k}"' for k in keys])
            valuespec = ", ".join(":{}".format(k) for k in keys)
            q = INSERT.format(table=table, colspec=colspec, valuespec=valuespec)
        else:
            q = INSERT_DEFAULT.format(table=table)

        if upsert_on:
            upsert_keys = list(keys)

            for k in upsert_on:
                upsert_keys.remove(k)

            upsertkeyspec = ", ".join([f'"{k}"' for k in upsert_on])

            if upsert_keys:
                upsertspec = ", ".join(f'"{k}" = excluded."{k}"' for k in upsert_keys)

                q_upsert = INSERT_UPSERT.format(
                    upsertkeyspec=upsertkeyspec, upsertspec=upsertspec
                )
            else:
                q_upsert = INSERT_UPSERT_DO_NOTHING.format(upsertkeyspec=upsertkeyspec)

            q = q + q_upsert
        if returning:
            q += " returning *"

        return self.qvalues(q, rows, fetch=bool(returning))
