"""A three-stage pipeline: generate -> square -> print.

Go version: https://go.dev/blog/pipelines
"""

from __future__ import annotations

import threading

from pygochan import Chan, Channel, In, Out, close, take


def generate(out: Channel[int, In], *numbers: int) -> None:
    for n in numbers:
        n >> out
    close(out)


def square(inp: Channel[int, Out], out: Channel[int, In]) -> None:
    for n in inp:
        (n * n) >> out
    close(out)


def main() -> None:
    numbers, squares = Chan(int), Chan(int)
    threading.Thread(target=generate, args=(numbers, 1, 2, 3, 4)).start()
    threading.Thread(target=square, args=(numbers, squares)).start()

    first = take << squares
    print("first:", first)
    for value in squares:
        print("next:", value)


if __name__ == "__main__":
    main()
