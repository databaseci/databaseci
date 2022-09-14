from urllib.parse import _coerce_args, urlsplit
from urllib.parse import urlunsplit as unsp

NETLOC_PARTS = "username password hostname port".split()

uses_netloc = [
    "",
    "ftp",
    "http",
    "gopher",
    "nntp",
    "telnet",
    "imap",
    "wais",
    "file",
    "mms",
    "https",
    "shttp",
    "snews",
    "prospero",
    "rtsp",
    "rtspu",
    "rsync",
    "svn",
    "svn+ssh",
    "sftp",
    "nfs",
    "git",
    "git+ssh",
    "ws",
    "wss",
]


def urlunsplit(components):
    """Combine the elements of a tuple as returned by urlsplit() into a
    complete URL as a string. The data argument can be any five-item iterable.
    This may result in a slightly different, but equivalent URL, if the URL that
    was parsed originally had unnecessary delimiters (for example, a ? with an
    empty query; the RFC states that these are equivalent)."""
    scheme, netloc, url, query, fragment, _coerce_result = _coerce_args(*components)

    if netloc or (scheme and url[:2] != "//"):
        if url and url[:1] != "/":
            url = "/" + url
        url = "//" + (netloc or "") + url
    if scheme:
        url = scheme + ":" + url
    if query:
        url = url + "?" + query
    if fragment:
        url = url + "#" + fragment
    return _coerce_result(url)


def combine_netloc_parts(parts):
    u = parts["username"]
    p = parts["password"]
    h = parts["hostname"]
    pp = parts["port"]

    login = u or ""

    if p:
        login = f"{login}:{p}"

    hpp = h or ""

    if hpp:
        if pp:
            hpp = f"{hpp}:{pp}"

    if login:
        hpp = f"{login}@{hpp}"

    return hpp


class URL:
    def __init__(self, s):
        self.s = s
        self.parts = self.urlsplit()

    def urlsplit(self):
        return urlsplit(self.s)

    def __getattr__(self, name):
        if hasattr(self.parts, name):
            return getattr(self.parts, name)

        if hasattr(self, name):
            return getattr(self, name)

        raise AttributeError

    def netloc_parts(self):
        return {k: getattr(self.parts, k) for k in NETLOC_PARTS}

    def replace_netloc_part(self, name, value):
        parts = self.netloc_parts()

        parts[name] = value

        combined = combine_netloc_parts(parts)

        self.replace_url_part("netloc", combined)

    def replace_url_part(self, name, value):
        self.parts = self.parts._replace(**{name: value})
        self.s = urlunsplit(self.parts)

    def __setattr__(self, name, value):
        if name in "s parts".split():
            super().__setattr__(name, value)
            return

        if name in NETLOC_PARTS:
            self.replace_netloc_part(name, value)
        elif name in self.parts._fields:
            self.replace_url_part(name, value)
        else:
            super().__setattr__(name, value)

    @property
    def absolute_path(self):
        _path = self.path
        if not _path.startswith("/"):
            _path = "/" + self.path
        return _path

    @property
    def relative_path(self):
        return self.path.lstrip("/")

    def __str__(self):
        return urlunsplit(self.parts)


def url(s):
    return URL(s)
