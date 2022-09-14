import random
import socket
import time
from contextlib import contextmanager
from string import ascii_lowercase

import sqlalchemy
from pendulum import now
from psycopg2 import OperationalError

import databaseci

from .database import db


def get_docker_client():
    import docker

    return docker.from_env()


def try_connect(db_url, timeout=3.0, should_raise=True):
    start = now()

    while True:
        try:
            with db(db_url).t() as t:
                t.q("select 1")
                return True
        except OperationalError:
            pass

        current = now()
        elapsed = current - start

        if elapsed.total_seconds() > timeout:
            if should_raise:
                raise RuntimeError("cannot connect")
            return False


def wait_open(port, host="localhost", timeout=3.0, should_raise=True):
    PAUSE = 0.1

    start = now()

    while True:

        try:
            s = socket.create_connection((host, port), timeout)
            s.close()
            return True
        except socket.error:
            pass

        elapsed = (now() - start).total_seconds()

        if elapsed > timeout:
            if should_raise:
                raise RuntimeError("timeout waiting for port")

            return False

        time.sleep(PAUSE)


def stop_container(container, wait=1):
    import docker

    try:
        container.stop(timeout=wait)
    except docker.errors.NotFound:
        pass


def stop_containers(prefix, wait=1):
    client = get_docker_client()
    for c in client.containers.list(ignore_removed=True):
        if c.name.startswith(prefix):
            stop_container(c, wait=wait)


def backoff(max_collisions=5, slot_time=0.1):
    for i in range(max_collisions):
        slots = 2**i - 1
        wait_seconds = slots * slot_time
        yield i, wait_seconds


def random_name(prefix):
    time_str = int(time.time())
    random_str = "".join([random.choice(ascii_lowercase) for _ in range(6)])

    return f"{prefix}-{time_str}-{random_str}"


@contextmanager
def temporary_local_db():
    db_name = random_name("tempdb-pg").replace("-", "_")

    home_db = databaseci.db()

    temp_db = home_db.create_db(db_name)

    try:
        yield temp_db

    finally:
        home_db.drop_db(db_name, yes_really_drop=True, force=True)


@contextmanager
def temporary_docker_db():
    container_name = random_name("tempdb-pg")

    client = get_docker_client()

    c = client.containers.run(
        "databaseci/tempdb-pg",
        name=container_name,
        detach=True,
        remove=True,
        ports={"5432/tcp": None},
    )
    try:
        for _ in backoff():
            c.reload()

            try:
                port = c.ports["5432/tcp"][0]["HostPort"]
                break
            except LookupError:
                continue

        db_url = f"postgresql://docker:pw@localhost:{port}/docker"

        wait_open(port, timeout=10)

        try_connect(db_url)
        yield db(db_url)

    finally:
        stop_container(c)


def pull_temporary_docker_db_image(version="latest"):
    print("pulling databaseci/tempdb-pg docker image...")
    client = get_docker_client()
    client.images.pull(f"databaseci/tempdb-pg:{version}")


def cleanup_temporary_docker_db_containers():
    stop_containers("tempdb-pg-")
