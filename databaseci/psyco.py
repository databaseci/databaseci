import re
import string

safe_chars = {k: None for k in (string.ascii_lowercase + "_")}


def reformat_bind_params(q, rewrite=True):
    x = re.compile(r"[^:](:[a-z0-9_]+)")

    parts = []

    remainder = 0

    for m in x.finditer(q):
        a, b = m.start(1), m.end(1)

        parts.append(q[remainder:a])

        if rewrite:
            varname = q[a + 1 : b]
            parts.append(f"%({varname})s")
        else:
            parts.append(q[a:b])

        remainder = b

    parts.append(q[remainder:])
    return "".join(parts)


def quoted_identifier(
    identifier, schema=None, identity_arguments=None, always_quote=False
):
    def qin(x):  # quote if needed
        if always_quote:
            return f'"{x}"'
        if all(_ in safe_chars for _ in x):
            return x
        else:
            return f'"{x}"'

    s = qin(identifier.replace('"', '""'))
    if schema:
        s = "{}.{}".format(qin(schema.replace('"', '""')), s)
    if identity_arguments is not None:
        s = "{}({})".format(s, identity_arguments)
    return s
