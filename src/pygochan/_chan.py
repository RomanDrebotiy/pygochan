"""The :class:`Chan` type, the :data:`take` receive placeholder and :func:`close`."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Generic, TypeVar as _PlainTypeVar, final, overload

from typing_extensions import TypeVar

from ._direction import Both, In, Out
from ._errors import ChannelDirectionError
from ._runtime import _ChanCore

__all__ = ["Chan", "Channel", "take", "close"]

T = TypeVar("T")
#: Channel direction. Covariant, so ``Chan[T, Both]`` is a subtype of both
#: ``Chan[T, In]`` and ``Chan[T, Out]``, just as Go converts a ``chan T``
#: to ``chan<- T`` and ``<-chan T`` implicitly.
D = TypeVar("D", bound="In | Out", covariant=True, default=Both)
_C = _PlainTypeVar("_C", bound="Chan[Any, Any]")


class Chan(Generic[T, D]):
    """A Go-style channel carrying values of type ``T``.

    A channel is created bidirectional. Values are sent with ``>>`` and
    received with `take` and ``<<``::

        c = Chan(float)        # unbuffered:  make(chan float64)
        c = Chan(float, 10)    # buffered:    make(chan float64, 10)

        2.5 >> c               # send:        c <- 2.5
        x = take << c          # receive:     x := <-c
        close(c)               # close:       close(c)

    Semantics follow Go exactly:

    * **Unbuffered** (``capacity == 0``): a send blocks until a receiver
      takes the value, and a receive blocks until a sender delivers one
      (a rendezvous).
    * **Buffered**: a send blocks only while the buffer is full, a receive
      only while it is empty. Values are delivered in FIFO order.
    * **Closed**: buffered values can still be received. After that every
      receive returns the zero value at once (see :meth:`recv_ok`). Sending
      on or closing a closed channel raises
      :class:`~pygochan.ClosedChannelError`.

    The *zero value* is ``elem_type()`` when it can be called without
    arguments (``0`` for ``int``, ``0.0`` for ``float``, ``""`` for
    ``str``, ``[]`` for ``list``), otherwise ``None``, Go's ``nil``.

    The second type parameter is the channel direction (:class:`In`,
    :class:`Out` or the default :class:`Both`). It is mainly used in
    annotations. :meth:`send_only` and :meth:`recv_only` create restricted
    views, which a type checker checks and the runtime enforces.

    ``Channel`` is an alias of ``Chan``. By convention ``Chan(...)`` creates
    channels and ``Channel[...]`` annotates them.

    Args:
        elem_type: The element type. It is used to compute the zero value
            and to infer ``T`` for static type checkers.
        capacity: Buffer size. ``0`` (the default) creates an unbuffered
            channel.

    Raises:
        TypeError: ``capacity`` is not an ``int``.
        ValueError: ``capacity`` is negative.
    """

    __slots__ = ("_core", "_can_send", "_can_recv")

    _core: _ChanCore
    _can_send: bool
    _can_recv: bool

    @overload
    def __init__(self, elem_type: type[T], capacity: int = 0) -> None: ...
    @overload
    def __init__(self, elem_type: object, capacity: int = 0) -> None: ...
    # The second overload accepts element types that are not classes, such
    # as ``int | None`` or ``Callable[[], int]``. ``T`` then comes from an
    # explicit parameter (``Chan[int | None](int | None)``) or from the
    # annotation of the target variable.

    def __init__(self, elem_type: object, capacity: int = 0) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise TypeError(f"capacity must be an int, not {type(capacity).__name__}")
        if capacity < 0:
            raise ValueError(f"capacity must be >= 0, got {capacity}")
        self._core = _ChanCore(elem_type, capacity)
        self._can_send = True
        self._can_recv = True

    @classmethod
    def _view(cls, core: _ChanCore, can_send: bool, can_recv: bool) -> Chan[Any, Any]:
        view = cls.__new__(cls)
        view._core = core
        view._can_send = can_send
        view._can_recv = can_recv
        return view

    # -- direction checks -------------------------------------------------

    def _check_send(self) -> None:
        if not self._can_send:
            raise ChannelDirectionError("send to receive-only channel")

    def _check_recv(self) -> None:
        if not self._can_recv:
            raise ChannelDirectionError("receive from send-only channel")

    # -- sending ----------------------------------------------------------

    def send(self: Chan[T, In], value: T) -> None:
        """Send ``value`` on the channel, blocking until it is accepted.

        Same as ``value >> c``. Use this method when the operator form is
        unavailable, for example for values whose own ``>>`` operator
        takes precedence (NumPy integers and arrays).

        Raises:
            ClosedChannelError: The channel is closed, or was closed while
                this call was blocked.
            ChannelDirectionError: This is a receive-only view.
        """
        self._check_send()
        self._core.send(value)

    def __rrshift__(self: Chan[T, In], value: T) -> None:
        """Send operator: ``value >> c`` is Go's ``c <- value``. See :meth:`send`."""
        self.send(value)

    def __rshift__(self: _C, other: Chan[_C, In]) -> None:
        """Send this channel into another one: ``c1 >> c2`` is Go's ``c2 <- c1``.

        This method is required. Python never calls a reflected operator
        (``__rrshift__``) when both operands have the same type, so without
        it ``c1 >> c2`` would raise ``TypeError``. Any non-channel right
        operand returns ``NotImplemented``.
        """
        if not isinstance(other, Chan):
            return NotImplemented
        other.send(self)

    # -- receiving --------------------------------------------------------

    def recv(self: Chan[T, Out]) -> T:
        """Receive a value, blocking until one is available.

        Same as ``take << c``. On a closed and drained channel this returns
        the zero value straight away. Use :meth:`recv_ok` to tell that case
        apart from a real value.

        Raises:
            ChannelDirectionError: This is a send-only view.
        """
        self._check_recv()
        return self._core.recv()[0]  # type: ignore[no-any-return]

    def recv_ok(self: Chan[T, Out]) -> tuple[T, bool]:
        """Receive a value together with a flag, Go's ``v, ok := <-c``.

        Returns:
            ``(value, True)`` for a value that was sent, or ``(zero, False)``
            if the channel is closed and has no buffered values left.

        Raises:
            ChannelDirectionError: This is a send-only view.
        """
        self._check_recv()
        return self._core.recv()

    def __iter__(self: Chan[T, Out]) -> Iterator[T]:
        """Iterate over received values until the channel is closed.

        This is Go's ``for v := range c``. The loop blocks while waiting
        for values and ends once the channel is closed and drained.

        Raises:
            ChannelDirectionError: This is a send-only view.
        """
        self._check_recv()
        return self._iterate()

    def _iterate(self) -> Iterator[Any]:
        recv = self._core.recv
        while True:
            value, ok = recv()
            if not ok:
                return
            yield value

    # -- closing ----------------------------------------------------------

    def close(self: Chan[Any, In]) -> None:
        """Close the channel. See :func:`pygochan.close`."""
        if not self._can_send:
            raise ChannelDirectionError("close of receive-only channel")
        self._core.close()

    # -- views ------------------------------------------------------------

    def send_only(self: Chan[T, In]) -> Chan[T, In]:
        """Return a send-only view (Go's ``chan<- T``) of this channel.

        The view shares the underlying channel. Receiving from it raises
        :class:`~pygochan.ChannelDirectionError`.
        """
        self._check_send()
        return self._view(self._core, can_send=True, can_recv=False)

    def recv_only(self: Chan[T, Out]) -> Chan[T, Out]:
        """Return a receive-only view (Go's ``<-chan T``) of this channel.

        The view shares the underlying channel. Sending to it or closing
        it raises :class:`~pygochan.ChannelDirectionError`.
        """
        self._check_recv()
        return self._view(self._core, can_send=False, can_recv=True)

    # -- introspection ----------------------------------------------------

    @property
    def cap(self) -> int:
        """Buffer capacity (Go's ``cap(c)``). ``0`` for an unbuffered channel."""
        return self._core.capacity

    @property
    def elem_type(self) -> Any:
        """The element type the channel was created with."""
        return self._core.elem_type

    @property
    def direction(self) -> type[In | Out]:
        """The runtime direction of this handle: :class:`Both`, :class:`In` or :class:`Out`."""
        if self._can_send and self._can_recv:
            return Both
        return In if self._can_send else Out

    def __len__(self) -> int:
        """Number of values currently buffered (Go's ``len(c)``).

        Like Go's ``len``, the result may be stale by the time it is used
        if other threads are using the channel at the same time.
        """
        return len(self._core.buffer)

    def __bool__(self) -> bool:
        """A channel is always truthy, even when its buffer is empty.

        Without this, ``if c:`` would fall back to :meth:`__len__` and treat
        an empty channel as false.
        """
        return True

    def __eq__(self, other: object) -> bool:
        """Two handles are equal if they refer to the same channel.

        As in Go, a direction view compares equal to the channel it came from.
        """
        if not isinstance(other, Chan):
            return NotImplemented
        return self._core is other._core

    def __hash__(self) -> int:
        return id(self._core)

    def __repr__(self) -> str:
        elem = getattr(self._core.elem_type, "__name__", None) or repr(self._core.elem_type)
        arrow = {Both: "chan", In: "chan<-", Out: "<-chan"}[self.direction]
        state = ", closed" if self._core.closed else ""
        return f"Chan({arrow} {elem}, len={len(self)}, cap={self.cap}{state})"


#: Alias of :class:`Chan`, intended for annotations: ``def f(c: Channel[int, In])``.
Channel = Chan


@final
class _Take:
    """Type of the :data:`take` receive placeholder. Do not instantiate."""

    __slots__ = ()

    def __lshift__(self, channel: Chan[T, Out]) -> T:
        """Receive operator: ``take << c`` is Go's ``<-c``. See :meth:`Chan.recv`."""
        if not isinstance(channel, Chan):
            return NotImplemented
        return channel.recv()

    def __repr__(self) -> str:
        return "take"

    def __reduce__(self) -> str:
        return "take"


#: Receive placeholder: ``x = take << c`` receives from ``c`` (Go's ``x := <-c``).
take: _Take = _Take()


def close(channel: Chan[Any, In]) -> None:
    """Close ``channel``, Go's ``close(c)``.

    After closing:

    * values already buffered can still be received;
    * then receives return the zero value straight away
      (``recv_ok`` reports ``ok=False``, iteration stops);
    * blocked receivers wake up with the zero value;
    * blocked senders wake up with :class:`~pygochan.ClosedChannelError`.

    Raises:
        ClosedChannelError: The channel is already closed.
        ChannelDirectionError: ``channel`` is a receive-only view.
    """
    channel.close()
