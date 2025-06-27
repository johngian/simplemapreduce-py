import typing
from multiprocessing import Queue as MPQueue

T = typing.TypeVar("T")


class TypedQueue(typing.Generic[T]):
    """A typed wrapper around multiprocessing.Queue"""

    def __init__(self, maxsize: int = 0, *, ctx=None):
        self._queue: MPQueue

        if ctx is None:
            self._queue = MPQueue(maxsize)
        else:
            self._queue = ctx.Queue(maxsize)

    def put(
        self, item: T, block: bool = True, timeout: typing.Optional[float] = None
    ) -> None:
        """Put an item into the queue"""
        self._queue.put(item, block, timeout)

    def get(self, block: bool = True, timeout: typing.Optional[float] = None) -> T:
        """Get an item from the queue"""
        return self._queue.get(block, timeout)


MapInputElement = typing.TypeVar("MapInputElement")
MappedInputElement = typing.TypeVar("MappedInputElement")
BatchProcessingList = typing.List[MapInputElement]
MapInputQueue = TypedQueue[typing.Union[MapInputElement, None]]
MapOutputQueue = TypedQueue[typing.Union[MappedInputElement, None]]
MapFnCallable = typing.Callable[[MapInputElement], MappedInputElement]
ReduceElement = typing.TypeVar("ReduceElement")
ReduceValue = typing.TypeVar("ReduceValue")
ReduceFnCallable = typing.Callable[
    [typing.Union[ReduceValue, None], ReduceElement], typing.Union[ReduceValue, None]
]
ReduceInputQueue = TypedQueue[typing.Union[ReduceElement, None]]
