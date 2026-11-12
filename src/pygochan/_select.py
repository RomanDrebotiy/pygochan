"""Go's ``select`` statement, designed to be used with ``match``."""

from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar, final, overload

from ._chan import Chan
from ._direction import In, Out
from ._runtime import SelectCase, select_cases

__all__ = ["select", "send", "Send"]

T = TypeVar("T")


@final
class Send(Generic[T]):
    """A send case for :func:`select`. Create it with :func:`send`.

    Creating a ``Send`` does **not** send anything. The value is sent only
    if ``select`` picks this case.
    """

    __slots__ = ("channel", "value")

    def __init__(self, channel: Chan[T, In] | None, value: T) -> None:
        self.channel = channel
        self.value = value

    def __repr__(self) -> str:
        return f"send({self.channel!r}, {self.value!r})"


def send(channel: Chan[T, In] | None, value: T) -> Send[T]:
    """Describe a send case for :func:`select`, Go's ``case c <- value:``.

    Example::

        match select(out=send(results, x), quit=done):
            case ("out", _, _): ...
            case ("quit", _, _): ...

    Args:
        channel: The channel to send on. ``None`` means a *nil* channel, so
            the case is never chosen. This is the Go idiom for disabling a
            case dynamically.
        value: The value to send if this case is chosen.
    """
    return Send(channel, value)


# The tuple returned when a case fires: (label, value, ok).
Selected = tuple[str, Any, bool]
_Case = Chan[Any, Out] | Send[Any] | None


@overload
def select(*, default: Literal[False] = ..., **cases: _Case) -> Selected: ...
@overload
def select(*, default: bool, **cases: _Case) -> Selected | None: ...


def select(*, default: bool = False, **cases: _Case) -> Selected | None:
    """Wait on several channel operations at once, Go's ``select``.

    Each keyword argument is a labelled case:

    * ``label=c``: receive from channel ``c``;
    * ``label=send(c, v)``: send ``v`` on channel ``c``;
    * ``label=None``: a *nil* case that is never chosen.

    ``select`` blocks until at least one case can proceed, performs exactly
    one of them (chosen at random if several are ready, as in Go) and
    returns a ``(label, value, ok)`` tuple:

    ===================================  =========  ==========
    case                                 ``value``  ``ok``
    ===================================  =========  ==========
    receive of a sent value              the value  ``True``
    receive from a closed channel        zero       ``False``
    send                                 ``None``   ``True``
    ===================================  =========  ==========

    With ``default=True`` the select never blocks. If no case is ready it
    returns ``None``, which is Go's ``default:`` branch.

    The result is designed for ``match``::

        match select(data=c1, quit=c2, out=send(c3, 42), default=True):
            case ("data", v, True):
                print("received", v)
            case ("data", _, False):
                print("c1 was closed")
            case ("quit", _, _):
                return
            case ("out", _, _):
                print("sent 42")
            case None:
                print("nothing ready")

    Labels are plain strings, so patterns compare them by value. Avoid
    ``case (c1, v, ok)``: in a pattern a bare name *captures* instead of
    comparing.

    With no non-nil cases, ``select()`` blocks forever (like Go's
    ``select {}``), or returns ``None`` straight away when ``default=True``.

    Args:
        default: If true, return ``None`` instead of blocking.
        **cases: The labelled cases.

    Returns:
        ``(label, value, ok)`` for the chosen case, or ``None`` if
        ``default`` is true and no case was ready.

    Raises:
        ClosedChannelError: The chosen case is a send on a closed channel.
        ChannelDirectionError: A receive case uses a send-only view, or a
            send case uses a receive-only view.
        TypeError: A case is not a channel, ``send(...)`` or ``None``.
    """
    labels: list[str] = []
    runtime_cases: list[SelectCase] = []
    for label, case in cases.items():
        if case is None:
            continue
        if isinstance(case, Send):
            channel = case.channel
            if channel is None:
                continue
            if not isinstance(channel, Chan):
                raise TypeError(f"select case {label!r}: send() target must be a Chan, not {type(channel).__name__}")
            channel._check_send()
            runtime_cases.append((channel._core, True, case.value))
        elif isinstance(case, Chan):
            case._check_recv()
            runtime_cases.append((case._core, False, None))
        else:
            raise TypeError(
                f"select case {label!r} must be a Chan (receive), send(...) or None, not {type(case).__name__}"
            )
        labels.append(label)

    result = select_cases(runtime_cases, block=not default)
    if result is None:
        return None
    index, value, ok = result
    return labels[index], value, ok
