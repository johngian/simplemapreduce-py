import multiprocessing
import typing
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Thread


class MapExecutor(Thread):
    def __init__(
        self,
        in_q: multiprocessing.Queue,
        out_q: multiprocessing.Queue,
        map_fn: typing.Callable,
        max_workers: int,
    ):
        super().__init__()
        self.map_q = in_q
        self.reduce_q = out_q
        self.map_fn = map_fn
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def run(self):
        while True:
            item = self.map_q.get()

            if item is None:
                break

            future = self.executor.submit(self.map_fn, item)
            self.reduce_q.put(future.result())


class ReduceExecutor(Thread):
    def __init__(self, in_q: multiprocessing.Queue, reduce_fn: typing.Callable):
        super().__init__()
        self.reduce_q = in_q
        self.reduce_fn = reduce_fn
        self.return_value = None

    def run(self):
        while True:
            item = self.reduce_q.get()

            if item is None:
                break

            self.return_value = self.reduce_fn(self.return_value, item)
