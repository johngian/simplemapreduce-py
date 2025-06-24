import multiprocessing
import typing
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Thread

ReduceReturnType = typing.TypeVar("ReduceReturnType")
ReduceInputType = typing.TypeVar("ReduceInputType")


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
    def __init__(
        self,
        in_q: multiprocessing.Queue,
        reduce_fn: typing.Callable[
            [ReduceInputType, ReduceInputType], ReduceReturnType
        ],
    ):
        super().__init__()
        self.reduce_q = in_q
        self.reduce_fn = reduce_fn
        self.value = None

    def run(self):
        while True:
            item = self.reduce_q.get()

            if item is None:
                break

            self.value = self.reduce_fn(self.value, item)

    def join(self, **kwargs) -> ReduceReturnType:
        super().join(**kwargs)
        return self.value
