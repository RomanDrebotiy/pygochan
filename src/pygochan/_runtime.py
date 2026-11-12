"""Low-level channel runtime: per-channel state, wait queues and select.

This module follows the design of Go's ``runtime/chan.go`` and
``runtime/select.go``:

* Every channel has its own lock (``_ChanCore.lock``), buffer and
  two FIFO wait queues: blocked senders (``sendq``) and blocked receivers
  (``recvq``).
* A blocked thread is represented by a :class:`_Parker`. It is Go's
  goroutine park/unpark pair plus the ``selectDone`` flag.
* Each queue entry is a :class:`_Waiter` (Go's ``sudog``). A thread
  blocked in ``select`` enqueues one waiter **per case**, and all of them
  share a single parker.
* Whoever wants to complete a waiter must first *claim* its parker. The
  claim is atomic and succeeds at most once, so exactly one case of a
  ``select`` can fire, however many channels race for it.

Note:

1. A plain send/receive/close holds exactly one channel lock.
2. ``select`` takes the locks of all its channels in a global order
   (by ``id``), the same way Go sorts by channel address.
3. A parker claim is a non-blocking ``Lock.acquire(False)``, so it never
   waits while a channel lock is held.
4. User code (computing zero values) never runs under a channel lock.

Nothing here is public API. The user-facing wrappers are in ``_chan.py``
and ``_select.py``.
"""

from __future__ import annotations

import random
import threading
from collections import deque
from collections.abc import Iterable, Sequence
from typing import Any, Final

from ._errors import ClosedChannelError

__all__: list[str] = []  # private module

# On the main thread a blocked wait is split into slices of this length
# so that Ctrl+C (KeyboardInterrupt) is delivered promptly on every
# platform. Waking a parked thread is still immediate: the slice only
# bounds how long a pending signal can be delayed.
_MAIN_THREAD_WAIT_SLICE: Final = 0.1


class _Parker:
    """One blocked operation (a plain send/receive, or a whole ``select``).

    The parker holds the outcome of the operation: which case fired, the
    value delivered to a receiver, and whether the operation succeeded
    (``ok``). The thread completing the operation claims the parker,
    fills in these fields and then wakes the parked thread. Because
    ``threading.Event.set`` and ``wait`` synchronize, the woken thread
    sees the fields fully written.
    """

    __slots__ = ("_claim", "_event", "case", "value", "ok")

    def __init__(self) -> None:
        # The claim flag is a lock that is acquired once and never
        # released: a non-blocking acquire is an atomic test-and-set.
        self._claim = threading.Lock()
        self._event = threading.Event()
        self.case: int = -1
        self.value: Any = None
        self.ok: bool = False

    def claim(self) -> bool:
        """Try to become the party that completes this operation.

        Returns ``True`` exactly once over the parker's lifetime. A losing
        caller must leave the waiter alone, because another channel (or
        the parked thread cancelling itself) has already taken it.
        """
        return self._claim.acquire(blocking=False)

    def wake(self, case: int, value: Any, ok: bool) -> None:
        """Publish the outcome and unpark the thread. Call only after :meth:`claim`."""
        self.case = case
        self.value = value
        self.ok = ok
        self._event.set()

    def wait(self) -> None:
        """Block until :meth:`wake` is called."""
        if threading.current_thread() is threading.main_thread():
            while not self._event.wait(_MAIN_THREAD_WAIT_SLICE):
                pass
        else:
            self._event.wait()


class _Waiter:
    """An entry in a channel wait queue (Go's ``sudog``).

    Attributes:
        parker: The blocked operation this entry belongs to.
        case: Index of the ``select`` case (always ``0`` for plain ops).
        value: For a waiting sender, the value it wants to send.
    """

    __slots__ = ("parker", "case", "value")

    def __init__(self, parker: _Parker, case: int, value: Any) -> None:
        self.parker = parker
        self.case = case
        self.value = value


# A registration of a waiter: the channel, the queue it sits in, the entry.
_Registration = tuple["_ChanCore", "deque[_Waiter]", _Waiter]

# Results of a non-blocking receive attempt.
_READY: Final = "ready"
_CLOSED: Final = "closed"
_NOT_READY: Final = "not-ready"


class _ChanCore:
    """The state shared by a channel and all of its direction views."""

    __slots__ = ("lock", "elem_type", "capacity", "buffer", "recvq", "sendq", "closed", "_zero_factory")

    def __init__(self, elem_type: Any, capacity: int) -> None:
        self.lock = threading.Lock()
        self.elem_type = elem_type
        self.capacity = capacity
        self.buffer: deque[Any] = deque()
        self.recvq: deque[_Waiter] = deque()
        self.sendq: deque[_Waiter] = deque()
        self.closed = False
        self._zero_factory: Any = elem_type if callable(elem_type) and elem_type is not object else None

    # -- zero value ------------------------------------------------------

    def zero(self) -> Any:
        """Return the zero value of the element type: ``elem_type()`` or ``None``.

        A new value is created on every call, so mutable zero values such
        as ``[]`` are never shared. If ``elem_type()`` fails once, the
        channel falls back to ``None`` from then on (Go's ``nil``). Never
        call this while holding a channel lock: it runs user code.
        """
        factory = self._zero_factory
        if factory is None:
            return None
        try:
            return factory()
        except Exception:
            self._zero_factory = None
            return None

    # -- non-blocking primitives (caller holds ``self.lock``) ------------

    @staticmethod
    def _pop_waiter(queue: deque[_Waiter]) -> _Waiter | None:
        """Dequeue the first waiter whose parker can still be claimed.

        Entries whose parker was already claimed (a ``select`` that fired
        on a different channel, or a cancelled wait) are dropped.
        """
        while queue:
            waiter = queue.popleft()
            if waiter.parker.claim():
                return waiter
        return None

    def try_send(self, value: Any) -> bool:
        """Attempt a send without blocking. Caller must hold ``self.lock``.

        Returns:
            ``True`` if the value was handed to a waiting receiver or
            buffered, ``False`` if the send would block.

        Raises:
            ClosedChannelError: The channel is closed.
        """
        if self.closed:
            raise ClosedChannelError("send on closed channel")
        receiver = self._pop_waiter(self.recvq)
        if receiver is not None:
            # Hand the value straight to the receiver (only possible when
            # the buffer is empty, otherwise nobody would be waiting).
            receiver.parker.wake(receiver.case, value, True)
            return True
        if len(self.buffer) < self.capacity:
            self.buffer.append(value)
            return True
        return False

    def try_recv(self) -> tuple[str, Any]:
        """Attempt a receive without blocking. Caller must hold ``self.lock``.

        Returns:
            ``(_READY, value)`` on success, ``(_CLOSED, None)`` if the
            channel is closed and drained (the caller produces the zero
            value outside the lock), or ``(_NOT_READY, None)``.
        """
        sender = self._pop_waiter(self.sendq)
        if sender is not None:
            if self.capacity == 0:
                # Unbuffered: take the value directly from the sender.
                value = sender.value
            else:
                # Buffer is full: take its head and move the sender's value
                # into the freed tail slot, preserving FIFO order.
                value = self.buffer.popleft()
                self.buffer.append(sender.value)
            sender.parker.wake(sender.case, None, True)
            return _READY, value
        if self.buffer:
            return _READY, self.buffer.popleft()
        if self.closed:
            return _CLOSED, None
        return _NOT_READY, None

    # -- blocking operations ----------------------------------------------

    def send(self, value: Any) -> None:
        """Send ``value``, blocking until a receiver or buffer slot takes it."""
        with self.lock:
            if self.try_send(value):
                return
            parker = _Parker()
            waiter = _Waiter(parker, 0, value)
            self.sendq.append(waiter)
        _park(parker, ((self, self.sendq, waiter),))
        if not parker.ok:
            # Woken by close() while blocked: Go panics here as well.
            raise ClosedChannelError("send on closed channel")

    def recv(self) -> tuple[Any, bool]:
        """Receive a value, blocking until one is available.

        Returns:
            ``(value, True)`` for a sent value, or ``(zero, False)`` once
            the channel is closed and drained.
        """
        with self.lock:
            status, value = self.try_recv()
            if status is _READY:
                return value, True
            waiter = _Waiter(_Parker(), 0, None) if status is _NOT_READY else None
            if waiter is not None:
                self.recvq.append(waiter)
        if waiter is None:  # closed and drained
            return self.zero(), False
        parker = waiter.parker
        _park(parker, ((self, self.recvq, waiter),))
        if parker.ok:
            return parker.value, True
        return self.zero(), False

    def close(self) -> None:
        """Close the channel and release every blocked sender and receiver.

        Raises:
            ClosedChannelError: The channel is already closed.
        """
        with self.lock:
            if self.closed:
                raise ClosedChannelError("close of closed channel")
            self.closed = True
            released = [w for q in (self.recvq, self.sendq) for w in q if w.parker.claim()]
            self.recvq.clear()
            self.sendq.clear()
        # Receivers get ok=False and build the zero value themselves.
        # Senders get ok=False and raise ClosedChannelError.
        for waiter in released:
            waiter.parker.wake(waiter.case, None, False)


# -- helpers shared by blocking operations ---------------------------------


def _lock_order(cores: Iterable[_ChanCore]) -> list[_ChanCore]:
    """Return the distinct channels in the global lock-acquisition order."""
    return sorted({id(c): c for c in cores}.values(), key=id)


def _acquire_all(cores: Sequence[_ChanCore]) -> None:
    for core in cores:
        core.lock.acquire()


def _release_all(cores: Sequence[_ChanCore]) -> None:
    for core in reversed(cores):
        core.lock.release()


def _unregister(registrations: Sequence[_Registration]) -> None:
    """Remove the still-queued waiters of a finished or cancelled operation."""
    cores = _lock_order(core for core, _, _ in registrations)
    _acquire_all(cores)
    try:
        for _, queue, waiter in registrations:
            try:
                queue.remove(waiter)
            except ValueError:
                pass  # already dequeued by the party that completed us
    finally:
        _release_all(cores)


def _park(parker: _Parker, registrations: Sequence[_Registration]) -> None:
    """Block the current thread until ``parker`` is woken.

    Once woken, waiters left in other queues are removed (only a
    ``select`` has more than one).

    If the wait is interrupted (for example by ``KeyboardInterrupt``), the
    thread tries to claim its own parker to cancel the operation. If a peer
    claimed it first, the operation has already completed and the
    interruption propagates anyway. A value received in that narrow window
    is lost, which is unavoidable because Python can interrupt any blocking
    call.
    """
    try:
        parker.wait()
    except BaseException:
        if not parker.claim():
            parker._event.wait()  # peer is publishing the result right now
        _unregister(registrations)
        raise
    if len(registrations) > 1:
        _unregister(registrations)


def block_forever() -> None:
    """Block the calling thread forever, like Go's empty ``select {}``."""
    _Parker().wait()


# -- select ------------------------------------------------------------------

#: A select case: (channel, is_send, value_to_send).
SelectCase = tuple[_ChanCore, bool, Any]


def select_cases(cases: Sequence[SelectCase], block: bool) -> tuple[int, Any, bool] | None:
    """Run Go's select algorithm over ``cases``.

    Args:
        cases: The non-nil cases. A receive case has ``is_send=False`` and
            its value is ignored.
        block: ``False`` when the select has a ``default`` branch.

    Returns:
        ``(case_index, value, ok)`` for the case that fired, or ``None``
        if no case was ready and ``block`` is false. For a receive from a
        closed channel the returned value is the zero value and ``ok`` is
        ``False``. For a send ``value`` is ``None`` and ``ok`` is ``True``.

    Raises:
        ClosedChannelError: The chosen case is a send on a closed channel.
    """
    if not cases:
        if not block:
            return None
        block_forever()

    # Poll the cases in random order so that no case is starved, as Go does.
    order = list(range(len(cases)))
    random.shuffle(order)

    cores = _lock_order(core for core, _, _ in cases)
    parker: _Parker | None = None
    registrations: list[_Registration] = []
    closed_index = -1
    _acquire_all(cores)
    try:
        # Pass 1: is any case ready right now?
        for index in order:
            core, is_send, value = cases[index]
            if is_send:
                if core.try_send(value):
                    return index, None, True
            else:
                status, received = core.try_recv()
                if status is _READY:
                    return index, received, True
                if status is _CLOSED:
                    closed_index = index
                    break
        else:
            if not block:
                return None
            # Pass 2: enqueue one waiter per case, all sharing one parker.
            parker = _Parker()
            for index, (core, is_send, value) in enumerate(cases):
                queue = core.sendq if is_send else core.recvq
                waiter = _Waiter(parker, index, value)
                queue.append(waiter)
                registrations.append((core, queue, waiter))
    finally:
        _release_all(cores)

    if parker is None:
        # A receive case found its channel closed: produce the zero value
        # outside the locks, because it may run user code.
        return closed_index, cases[closed_index][0].zero(), False

    # Pass 3: sleep until one case fires, then dequeue from the others.
    _park(parker, registrations)
    core, is_send, _ = cases[parker.case]
    if parker.ok:
        return parker.case, parker.value, True
    if is_send:
        raise ClosedChannelError("send on closed channel")
    return parker.case, core.zero(), False
