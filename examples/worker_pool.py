"""A worker pool with buffered job and result channels.

Go version: https://gobyexample.com/worker-pools
"""

from __future__ import annotations

import threading
import time

from pygochan import Chan, Channel, In, Out, close, take


def worker(wid: int, jobs: Channel[int, Out], results: Channel[str, In]) -> None:
    for job in jobs:
        time.sleep(0.05)  # pretend to work
        f"worker {wid} finished job {job}" >> results


def main() -> None:
    n_jobs = 9
    jobs, results = Chan(int, n_jobs), Chan(str, n_jobs)

    for wid in range(3):
        threading.Thread(target=worker, args=(wid, jobs, results)).start()

    for job in range(n_jobs):
        job >> jobs
    close(jobs)

    for _ in range(n_jobs):
        print(take << results)


if __name__ == "__main__":
    main()
