import typing
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

import simplemapreduce.types


class MapProcessing(Thread):
    """
    Map processing thread class.

    This class provides an abstraction to start a concurrent thread pool executor
    It consumes messages from an input queue, maps a function to each one and puts
    function result to an output queue. Stops reading from the queue when it reaches
    an item of type None.

    Args:
        in_q: Processing input queue
        out_q: Processing output queue
        map_fn: Callable to apply on each item
        batch_size: Size of the processing batch
        max_workers: Size of thread pool executor
    """

    def __init__(
        self,
        in_q: simplemapreduce.types.MapInputQueue,
        out_q: simplemapreduce.types.MapOutputQueue,
        map_fn: typing.Callable[
            [simplemapreduce.types.MapInputElement],
            simplemapreduce.types.MappedInputElement,
        ],
        batch_size: int,
        max_workers: int,
    ):
        super().__init__()
        self.in_q = in_q
        self.out_q = out_q
        self.batch_size = batch_size
        self.batch: simplemapreduce.types.BatchProcessingList = []
        self.map_fn = map_fn
        self.max_workers = max_workers

    def flush(self):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for mapped in executor.map(self.map_fn, self.batch):
                self.out_q.put(mapped)
        self.batch = []

    def add(self, item):
        self.batch.append(item)
        if len(self.batch) > self.batch_size:
            self.flush()

    def run(self):
        while True:
            item = self.in_q.get()

            if item is None:
                break

            self.add(item)

        self.flush()
        self.out_q.put(None)


class ReduceProcessing(Thread):
    """
    Reduce processing thread class.

    This class provides an abstraction for a thread that reduces the elements
    of an input queue based on a reduce function.

    Args:
        in_q: Processing input queue
        reduce_fn: Reduce function to apply on each item. Signature: (acc, value) -> reduced.

    Attributes:
        return_value: Value of the reduced operation
    """

    def __init__(
        self,
        in_q: simplemapreduce.types.ReduceInputQueue,
        reduce_fn: simplemapreduce.types.ReduceFnCallable,
    ):
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
