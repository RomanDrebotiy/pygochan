"""Exceptions raised by :mod:`pygochan`.

Go reports channel misuse with a runtime *panic*. pygochan raises the
exceptions below in the same situations.
"""

from __future__ import annotations

__all__ = ["ChannelError", "ClosedChannelError", "ChannelDirectionError"]


class ChannelError(RuntimeError):
    """Base class for every error raised by pygochan."""


class ClosedChannelError(ChannelError):
    """An operation is not allowed on a closed channel.

    Raised in the same cases where Go panics:

    * sending on a closed channel (``send on closed channel``), including
      a sender that was blocked when the channel got closed;
    * closing a channel that is already closed
      (``close of closed channel``);
    * a ``select`` that picks a send case whose channel is closed.

    Receiving from a closed channel is **not** an error. It returns the
    channel's zero value straight away, as in Go.
    """


class ChannelDirectionError(ChannelError, TypeError):
    """The operation goes against the direction of a channel view.

    Raised when receiving from a send-only (``In``) view, or when sending
    to or closing a receive-only (``Out``) view. Go rejects these at
    compile time. A static type checker reports them for pygochan too,
    and this exception is the runtime safety net.
    """
