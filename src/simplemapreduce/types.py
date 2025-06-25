import typing
from multiprocessing.queues import Queue

MapInputElement = typing.TypeVar("MapInputElement")
MappedInputElement = typing.TypeVar("MappedInputElement")
BatchProcessingList = typing.List[MapInputElement]
MapInputQueue = Queue[MapInputElement | None]
MapOutputQueue = Queue[MappedInputElement | None]
MapFnCallable = typing.Callable[[MapInputElement], MappedInputElement]
ReduceElement = typing.TypeVar("ReduceElement")
ReduceValue = typing.TypeVar("ReduceValue")
ReduceFnCallable = typing.Callable[[ReduceValue | None, ReduceElement], ReduceValue]
ReduceInputQueue = Queue[ReduceElement | None]
