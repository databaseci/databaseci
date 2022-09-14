import errno
import fcntl
import logging
import os
import select
import signal
import sys
from contextlib import contextmanager

import psycopg2

from .psyco import quoted_identifier

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_wakeup_fd():
    pipe_r, pipe_w = os.pipe()
    flags = fcntl.fcntl(pipe_w, fcntl.F_GETFL, 0)
    flags = flags | os.O_NONBLOCK
    flags = fcntl.fcntl(pipe_w, fcntl.F_SETFL, flags)

    signal.set_wakeup_fd(pipe_w)
    return pipe_r


def empty_signal_handler(signal, frame):
    pass


def start_listening(connection, channels):
    names = [quoted_identifier(each) for each in channels]

    c = connection.cursor()

    for name in names:
        c.execute(f"listen {name};")
    c.close()


def log_notification(_n):
    log.debug("NOTIFY: {}, {}, {}".format(_n.pid, _n.channel, _n.payload))


class ListenNotify:
    def notifications(
        self, connection, timeout=5, yield_on_timeout=False, handle_signals=None
    ):
        """Subscribe to PostgreSQL notifications, and handle them
        in infinite-loop style.

        On an actual message, returns the notification (with .pid,
        .channel, and .payload attributes).

        If you've enabled 'yield_on_timeout', yields None on timeout.
        """

        cc = connection

        timeout_is_callable = callable(timeout)

        signals_to_handle = handle_signals or []
        original_handlers = {}

        try:
            if signals_to_handle:
                for s in signals_to_handle:
                    original_handlers[s] = signal.signal(s, empty_signal_handler)
                wakeup = get_wakeup_fd()
                listen_on = [cc, wakeup]
            else:
                listen_on = [cc]
                wakeup = None

            while True:
                try:
                    if timeout_is_callable:
                        _timeout = timeout()
                        log.debug("dynamic timeout of {_timeout} seconds")
                    else:
                        _timeout = timeout
                    _timeout = max(0, _timeout)

                    r, w, x = select.select(listen_on, [], [], _timeout)
                    log.debug("select call awoken, returned: {}".format((r, w, x)))

                    if (r, w, x) == ([], [], []):
                        log.debug("idle timeout on select call, carrying on...")
                        if yield_on_timeout:
                            yield None

                    if wakeup is not None and wakeup in r:
                        signal_byte = os.read(wakeup, 1)
                        signal_int = int.from_bytes(signal_byte, sys.byteorder)
                        sig = signal.Signals(signal_int)
                        signal_name = signal.Signals(sig).name

                        log.info(f"woken from slumber by signal: {signal_name}")
                        yield signal_int

                    if cc in r:
                        cc.poll()

                        while cc.notifies:
                            notify = cc.notifies.pop()
                            yield notify

                except select.error as e:
                    e_num, e_message = e
                    if e_num == errno.EINTR:
                        log.debug("EINTR happened during select")
                    else:
                        raise
        finally:
            if signals_to_handle:
                for s in signals_to_handle:
                    if s in original_handlers:
                        signal_name = signal.Signals(s).name
                        log.debug(f"restoring original handler for: {signal_name}")
                        signal.signal(s, original_handlers[s])

    @contextmanager
    def listen(self, channels):
        with self.c_autocommit() as cc:
            if isinstance(channels, str):
                channels = [channels]
            start_listening(cc, channels)
            yield cc
