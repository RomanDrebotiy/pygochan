"""``select`` with ``match``: timeouts, a quit signal and non-blocking polls.

Go versions: https://gobyexample.com/timeouts and
https://gobyexample.com/non-blocking-channel-operations
"""

from __future__ import annotations

import threading
import time

from pygochan import Chan, Channel, close, select, send


def after(seconds: float) -> Channel[float]:
    """Return a channel that receives the current time once ``seconds`` have passed.

    A small equivalent of Go's ``time.After``.
    """
    c = Chan(float, 1)
    threading.Timer(seconds, lambda: time.monotonic() >> c).start()
    return c


def slow_result(out: Channel[str], delay: float) -> None:
    time.sleep(delay)
    "result" >> out


def main() -> None:
    # 1. Timeout: the result arrives too late.
    res = Chan(str, 1)
    threading.Thread(target=slow_result, args=(res, 0.5)).start()
    match select(res=res, timeout=after(0.1)):
        case ("res", value, _):
            print("got", value)
        case ("timeout", _, _):
            print("timeout 1")

    # 2. The result arrives in time.
    res = Chan(str, 1)
    threading.Thread(target=slow_result, args=(res, 0.05)).start()
    match select(res=res, timeout=after(1.0)):
        case ("res", value, _):
            print("got", value)
        case ("timeout", _, _):
            print("timeout 2")

    # 3. Non-blocking send and receive via default=True.
    messages = Chan(str)
    match select(msg=messages, default=True):
        case ("msg", m, _):
            print("received", m)
        case None:
            print("no message received")
    match select(out=send(messages, "hi"), default=True):
        case ("out", _, _):
            print("sent message")
        case None:
            print("no message sent")

    # 4. A loop that stops on a quit signal or when the data channel closes.
    data, quit_ = Chan(int, 10), Chan(bool)
    for i in range(3):
        i >> data
    close(data)
    while True:
        match select(data=data, quit=quit_):
            case ("data", v, True):
                print("data", v)
            case ("data", _, False):
                print("data channel closed")
                break
            case ("quit", _, _):
                break


if __name__ == "__main__":
    main()
