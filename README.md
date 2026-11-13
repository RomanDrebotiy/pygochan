# pygochan

**Go-style channels for Python threads**, with a small operator syntax that
mirrors Go's `<-` and a `select` that works with Python's `match`.

```python
from pygochan import Chan, take, close

c = Chan(float, 10)   # make(chan float64, 10)
2.5 >> c              # c <- 2.5
x = take << c         # x := <-c
close(c)              # close(c)
```

* Same semantics as Go: unbuffered rendezvous, buffered FIFO, close,
  zero values, `range`, `select` with random fairness, and nil channels.
* Typed: `Channel[float]`, `Channel[float, In]` (send-only) and
  `Channel[float, Out]` (receive-only) are checked by mypy and pyright.
  Direction views are also enforced at runtime.
* Built on `threading`. The only dependency is `typing_extensions`, needed
  on Python versions before 3.13.

---

## Contents

- [Installation](#installation)
- [Go to pygochan cheat sheet](#go-to-pygochan-cheat-sheet)
- [Quick start](#quick-start)
- [Guide](#guide)
  - [Creating channels](#creating-channels)
  - [Sending and receiving](#sending-and-receiving)
  - [Closing, `recv_ok` and iteration](#closing-recv_ok-and-iteration)
  - [Zero values](#zero-values)
  - [Channel directions and typing](#channel-directions-and-typing)
  - [`select` with `match`](#select-with-match)
  - [`len`, `cap` and other helpers](#len-cap-and-other-helpers)
  - [Errors](#errors)
- [Notes](#notes)
- [API reference](#api-reference)
- [Development](#development)
- [License](#license)

---

## Installation

pygochan needs **Python 3.10+** (`select` is designed around `match`).

```bash
pip install ./pygochan             # from a checkout of this repository
pip install -e "./pygochan[dev]"   # editable, with type-check tools
```

## Go to pygochan cheat sheet

| Go                              | pygochan                                     |
|---------------------------------|----------------------------------------------|
| `c := make(chan int)`           | `c = Chan(int)`                              |
| `c := make(chan int, 10)`       | `c = Chan(int, 10)`                          |
| `c <- v`                        | `v >> c`  or  `c.send(v)`                    |
| `v := <-c`                      | `v = take << c`  or  `v = c.recv()`          |
| `v, ok := <-c`                  | `v, ok = c.recv_ok()`                        |
| `close(c)`                      | `close(c)`                                   |
| `for v := range c { … }`        | `for v in c: …`                              |
| `len(c)`, `cap(c)`              | `len(c)`, `c.cap`                            |
| `chan int` (type)               | `Channel[int]`                               |
| `chan<- int` (send-only)        | `Channel[int, In]`                           |
| `<-chan int` (receive-only)     | `Channel[int, Out]`                          |
| `var c chan int` (nil channel)  | `None` as a `select` case                    |
| `select { case v := <-a: … }`   | `match select(a=a): case ("a", v, ok): …`    |
| `select { case b <- v: … }`     | `match select(b=send(b, v)): case ("b", _, _): …` |
| `select { … default: … }`       | `match select(…, default=True): … case None: …` |

## Quick start

```python
import threading
from pygochan import Chan, Channel, In, Out, close, take


def generate(out: Channel[int, In], *numbers: int) -> None:
    for n in numbers:
        n >> out
    close(out)


def square(inp: Channel[int, Out], out: Channel[int, In]) -> None:
    for n in inp:            # ends when `inp` is closed
        (n * n) >> out
    close(out)


numbers, squares = Chan(int), Chan(int)
threading.Thread(target=generate, args=(numbers, 1, 2, 3, 4)).start()
threading.Thread(target=square, args=(numbers, squares)).start()

print(take << squares)       # 1
print(list(squares))         # [4, 9, 16]
```

More runnable programs are in [`examples/`](examples):

| File | Shows |
|------|-------|
| [`pipeline.py`](examples/pipeline.py) | multi-stage pipeline with typed directions |
| [`worker_pool.py`](examples/worker_pool.py) | buffered job and result channels, several workers |
| [`select_timeout.py`](examples/select_timeout.py) | `select` + `match`: timeouts, non-blocking ops, quit signals |

---

## Guide

### Creating channels

```python
c = Chan(float)        # unbuffered
c = Chan(float, 10)    # buffered, capacity 10
```

`Chan(elem_type, capacity=0)` takes the element type and an optional
buffer size:

* **Unbuffered** (`capacity == 0`): every send waits for a receiver and every
  receive waits for a sender, so the two threads meet (*rendezvous*).
* **Buffered**: a send blocks only while the buffer is full, a receive only
  while it is empty. Values come out in the order they went in.

The element type gives the channel its [zero value](#zero-values) and lets
type checkers infer `T`. Values are **not** type-checked at runtime, just as
any Python function with annotations.

### Sending and receiving

```python
2.5 >> c          # send
x = take << c     # receive
```

The arrows point the way the data flows: the value flows *into* `c`, and the
received value flows *out of* `c` into `take`. `take` is a placeholder
object exported by pygochan. Its only purpose is to make `<<` read as Go's
receive operator.

Every operator has a method equivalent:

```python
c.send(2.5)
x = c.recv()
```

Channels of channels work as in Go:

```python
replies = Chan(Chan[str], 1)
reply = Chan(str, 1)
reply >> replies          # sends the channel `reply` into `replies`
r = take << replies       # r is reply
```

### Closing, `recv_ok` and iteration

```python
close(c)        # or c.close()
```

After `close(c)`, the behaviour matches Go:

| Operation                             | Result                                     |
|---------------------------------------|--------------------------------------------|
| receive while values are buffered     | the next buffered value                    |
| receive when drained                  | **zero value straight away**               |
| `c.recv_ok()`                         | `(value, True)`, or `(zero, False)` once drained |
| `for v in c:`                         | stops once drained                         |
| receivers blocked on `c`              | wake up with the zero value                |
| send (`v >> c`)                       | `ClosedChannelError`                       |
| senders blocked on `c`                | wake up with `ClosedChannelError`          |
| `close(c)` again                      | `ClosedChannelError`                       |

The usual consumer loop is Go's `for v := range c`:

```python
for job in jobs:
    handle(job)
```

### Zero values

Receiving from a closed and drained channel returns the **zero value** of
the element type:

* `elem_type()` if it can be called without arguments: `0`, `0.0`, `""`,
  `False`, `b""`, `[]`, `{}`, `()`, an instance of your dataclass with
  defaults, and so on. A **new** object is created each time, so mutable
  zero values are never shared.
* otherwise `None` (Go's `nil`). This covers `object`, unions like
  `int | None`, channels and classes with required arguments.

Use `recv_ok()` (or a `select` result's `ok`) to tell a real `0` apart from
"closed".

### Channel directions and typing

`Channel` is an alias of `Chan`. By convention `Chan(...)` *creates* a
channel and `Channel[...]` *annotates* one.

```python
from pygochan import Channel, In, Out, Both

def producer(c: Channel[float, In]) -> None: ...   # chan<- float64: send and close
def consumer(c: Channel[float, Out]) -> None: ...  # <-chan float64: receive only
def both(c: Channel[float]) -> None: ...           # chan float64 (same as Channel[float, Both])
```

Read the markers from the channel's point of view: `In` means values go
**in**, `Out` means values come **out**.

As in Go, a bidirectional channel can be passed wherever a directional one
is expected (`Both` is a subclass of `In` and `Out`, and the direction
parameter is covariant). The opposite conversion is rejected:

```python
c = Chan(float)
producer(c)          # OK
consumer(c)          # OK

def bad(c: Channel[float, Out]) -> None:
    1.0 >> c         # mypy/pyright: error, cannot send on a receive-only channel
    close(c)         # mypy/pyright: error
```

**Runtime enforcement.** Annotations alone do not change the object. To
pass on a handle that is restricted at runtime too, create a *view*:

```python
tx = c.send_only()   # Chan[float, In]
rx = c.recv_only()   # Chan[float, Out]

take << tx           # ChannelDirectionError: receive from send-only channel
1.0 >> rx            # ChannelDirectionError: send to receive-only channel
close(rx)            # ChannelDirectionError: close of receive-only channel
```

Views share the underlying channel: `c == tx == rx`, and closing `tx` closes
`c`. `c.direction` returns `Both`, `In` or `Out`.

**Element types that are not classes** (unions, `Callable`, and so on) need
an explicit type parameter or an annotated target:

```python
a = Chan[int | None](int | None)
b: Channel[int | None] = Chan(int | None)
```

### `select` with `match`

`select` waits on several channel operations and runs **exactly one** of
them. Cases are keyword arguments, and the keyword is the case's label:

| Case                     | Meaning                                  |
|--------------------------|------------------------------------------|
| `label=c`                | receive from `c`                         |
| `label=send(c, v)`       | send `v` on `c`                          |
| `label=None`, `send(None, v)` | nil channel: never chosen (disables the case) |
| `default=True`           | do not block, return `None` if nothing is ready |

It returns a `(label, value, ok)` tuple, designed for `match`:

```python
from pygochan import select, send

match select(data=c1, quit=c2, out=send(c3, 42)):
    case ("data", v, True):
        print("received", v)
    case ("data", _, False):
        print("c1 is closed")
    case ("quit", _, _):
        return
    case ("out", _, _):
        print("sent 42")
```

| Case that fired               | `value`     | `ok`    |
|-------------------------------|-------------|---------|
| receive of a sent value       | the value   | `True`  |
| receive from a closed channel | zero value  | `False` |
| send                          | `None`      | `True`  |

Same rules as Go:

* If several cases are ready, one is chosen **uniformly at random**, so no
  case is starved.
* A send case whose channel is closed raises `ClosedChannelError`.
* `select()` with no (non-nil) cases blocks forever, like `select {}`.

Non-blocking operations use `default=True`:

```python
match select(msg=messages, default=True):
    case ("msg", m, _):
        print("received", m)
    case None:
        print("no message")
```

A timeout pattern (see [`examples/select_timeout.py`](examples/select_timeout.py)
for an `after()` helper like Go's `time.After`):

```python
match select(res=results, timeout=after(1.0)):
    case ("res", r, _): use(r)
    case ("timeout", _, _): print("timed out")
```

> **Why string labels?** In a `match` pattern a bare name *captures* rather
> than compares, so `case (c1, v, ok):` would match *anything* and rebind
> `c1`. String literals are compared by value, which makes them the right
> choice for case labels.

### `len`, `cap` and other helpers

```python
c = Chan(int, 5)
1 >> c
len(c)      # 1, the number of buffered values (Go's len)
c.cap       # 5, the capacity (Go's cap)
bool(c)     # always True, even when empty
c.elem_type # int
repr(c)     # 'Chan(chan int, len=1, cap=5)'
```

### Errors

All exceptions derive from `pygochan.ChannelError` (a `RuntimeError`).

| Exception                | Raised when (Go equivalent)                                  |
|--------------------------|--------------------------------------------------------------|
| `ClosedChannelError`     | send on closed channel, close of closed channel (Go panics) |
| `ChannelDirectionError`  | misusing a direction view (Go compile error); also a `TypeError` |

---

## Notes

* **Operator precedence.** `<<` and `>>` bind more tightly than comparisons
  and `&`/`|`, and less tightly than `+`/`-`. `x + 1 >> c` sends `x + 1`, but
  `take << c + 1` means `take << (c + 1)`. Use parentheses:
  `(take << c) + 1`.
* **Values with their own `>>`.** Python tries the *left* operand first. For
  values whose `__rshift__` accepts anything (for example NumPy integers
  and arrays, which broadcast), `v >> c` does not reach the channel. Use
  `c.send(v)` for such values. Channels as values are handled: `c1 >> c2`
  sends `c1` into `c2`.
* **Pyright's `reportUnusedExpression`.** `v >> c` is an expression
  statement, so pyright warns about it by default. Turn the rule off for
  code that uses the DSL:

  ```toml
  [tool.pyright]
  reportUnusedExpression = false
  ```

  mypy does not complain.
* **Why `take` and not `_`.** `_` is overwritten by IPython and Jupyter
  after every output, reassigned by `for _ in ...`, and used by `gettext`.
  If you misuse `take`, for example by shadowing it with a number, you get
  a clear `TypeError`, never a silent wrong result.
* **Threads, not goroutines.** Each concurrent task is an OS thread. That
  is fine for I/O-bound work and for hundreds or a few thousand tasks, but
  it is not Go's millions of goroutines. Pure-Python CPU work is still
  limited by the GIL (except on free-threaded builds).
* **Ctrl+C.** A blocked operation on the main thread can be interrupted with
  `KeyboardInterrupt`. The operation is then cancelled cleanly.

---

## API reference

Everything is importable from `pygochan`. Every public function and class
has a full docstring, so `help(pygochan.select)` works.

| Name | Kind | Summary |
|------|------|---------|
| `Chan(elem_type, capacity=0)` | class | A channel. Bidirectional when created. |
| `Channel` | alias | Same as `Chan`, intended for annotations. |
| `In`, `Out`, `Both` | markers | Direction type arguments: send-only, receive-only, bidirectional (the default). |
| `take` | placeholder | `take << c` receives from `c`. |
| `close(c)` | function | Close a channel. |
| `select(*, default=False, **cases)` | function | Go's `select`. Returns `(label, value, ok)` or `None`. |
| `send(c, v)` | function | Describe a send case for `select`. |
| `Send` | class | Type returned by `send()`. |
| `ChannelError`, `ClosedChannelError`, `ChannelDirectionError` | exceptions | See [Errors](#errors). |

`Chan` methods and properties:

| Member | Summary |
|--------|---------|
| `v >> c`, `c.send(v)` | Send `v`, blocking until accepted. |
| `take << c`, `c.recv()` | Receive, blocking; zero value once closed and drained. |
| `c.recv_ok()` | Receive as `(value, ok)`. |
| `for v in c` | Receive until closed and drained. |
| `c.close()` | Same as `close(c)`. |
| `c.send_only()`, `c.recv_only()` | Direction views sharing the same channel. |
| `len(c)`, `c.cap` | Buffered values and capacity. |
| `c.elem_type`, `c.direction` | Element type and runtime direction (`Both`, `In`, `Out`). |


## Declaration of generative AI technologies usage

Originally proposed by the author without using AI:
- idea of the library which mimics the Go channel semantics with the DSL based on shift operators
- match/select semantics similar to Go's
- general approach with the use of typical concurrency patterns with queues, threads, locks
- general idea of using similar architecture as in the original Go runtime

Generative AI (Anthropic Claude) was used as an assistant in writing the Python code, documentation and exploring the Go runtime.

After using this tool, the author reviewed, edited, and tested the resulting library code.

## License

*Copyright (c) 2026 Roman Drebotiy*

*Licensed under the Apache License 2.0 (see LICENSE file)*