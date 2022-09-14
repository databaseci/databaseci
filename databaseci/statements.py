from textwrap import indent


def create_table_statement(name, columns, extra=None, *, uuid_pk=None):
    colspec = []

    extra = extra or {}

    colspec = [dict(name=k, coltype=v, extra=extra.get(k)) for k, v in columns.items()]

    if uuid_pk:
        colspec = [
            dict(
                name=uuid_pk,
                coltype="uuid",
                extra="default gen_random_uuid() primary key",
            )
        ] + colspec

    def col_line(d):
        line = f"{d['name']} {d['coltype']}"

        if extra := d.get("extra"):
            line += f" {extra}"

        return line

    col_lines = [col_line(_) for _ in colspec]

    col_text = ",\n".join(col_lines)

    col_text = indent(col_text, "  ")

    statement = f"""\
create table {name} (
{col_text}
);

"""
    return statement
