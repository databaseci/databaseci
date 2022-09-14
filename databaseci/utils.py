import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Union

import yaml


def normalized_path(path_or_str: Union[Path, str]):
    p = Path(path_or_str)
    return p.expanduser()


def yaml_from_file(path):
    p = normalized_path(path)

    with p.open() as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError:
            raise
