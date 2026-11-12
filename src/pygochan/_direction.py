"""Direction markers for channel types.

These classes are never instantiated. They serve as the second type
argument of :class:`~pygochan.Channel`:

=========================  ==================  ===========================
pygochan                   Go                  allowed operations
=========================  ==================  ===========================
``Channel[T]``             ``chan T``          send, receive, close
``Channel[T, Both]``       ``chan T``          send, receive, close
``Channel[T, In]``         ``chan<- T``        send, close
``Channel[T, Out]``        ``<-chan T``        receive
=========================  ==================  ===========================

Read the names from the channel's point of view: ``In`` means values go
*into* the channel, ``Out`` means values come *out* of it.

``Both`` subclasses ``In`` and ``Out``, and the direction parameter of
``Channel`` is covariant. As in Go, a bidirectional channel can therefore
be passed wherever a send-only or receive-only channel is expected, but
not the other way round.
"""

from __future__ import annotations

__all__ = ["In", "Out", "Both"]


class In:
    """Marker for a send-only channel, Go's ``chan<- T``.

    ``Channel[T, In]`` can send (``value >> c``) and close, but cannot
    receive.
    """

    def __init__(self) -> None:
        raise TypeError(f"{type(self).__name__} is a type marker and cannot be instantiated")


class Out:
    """Marker for a receive-only channel, Go's ``<-chan T``.

    ``Channel[T, Out]`` can receive (``take << c``, iteration) but cannot
    send or close.
    """

    def __init__(self) -> None:
        raise TypeError(f"{type(self).__name__} is a type marker and cannot be instantiated")


class Both(In, Out):
    """Marker for a bidirectional channel, Go's ``chan T``.

    This is the default direction, so ``Channel[T]`` means
    ``Channel[T, Both]``.
    """
