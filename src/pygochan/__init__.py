"""pygochan: Go-style channels for Python threads.

Quick tour::

    import threading
    from pygochan import Chan, Channel, In, Out, take, close, select, send

    def producer(out: Channel[int, In]) -> None:
        for i in range(3):
            i >> out              # out <- i
        close(out)

    c = Chan(int)                 # make(chan int)
    threading.Thread(target=producer, args=(c,)).start()

    for v in c:                   # for v := range c
        print(v)

See the README for the full guide.
"""

from __future__ import annotations

from ._chan import Chan, Channel, close, take
from ._direction import Both, In, Out
from ._errors import ChannelDirectionError, ChannelError, ClosedChannelError
from ._select import Send, select, send

__version__ = "0.1.0"

__all__ = [
    "Chan",
    "Channel",
    "In",
    "Out",
    "Both",
    "take",
    "close",
    "select",
    "send",
    "Send",
    "ChannelError",
    "ClosedChannelError",
    "ChannelDirectionError",
    "__version__",
]
