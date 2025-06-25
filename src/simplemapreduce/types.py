import typing
from multiprocessing.queues import Queue

T = typing.TypeVar("T")


class TypedQueue(Queue, typing.Generic[T]):
    def put(self, obj: T, *args, **kwargs) -> None:
        super().put(obj, *args, **kwargs)

    def get(self, *args, **kwargs) -> T:
        return super().get(*args, **kwargs)


MapInputElement = typing.TypeVar("MapInputElement")
MappedInputElement = typing.TypeVar("MappedInputElement")
BatchProcessingList = typing.List[MapInputElement]
MapInputQueue = TypedQueue[typing.Union[MapInputElement, None]]
MapOutputQueue = TypedQueue[typing.Union[MappedInputElement, None]]
MapFnCallable = typing.Callable[[MapInputElement], MappedInputElement]
ReduceElement = typing.TypeVar("ReduceElement")
ReduceValue = typing.TypeVar("ReduceValue")
ReduceFnCallable = typing.Callable[
    [typing.Union[ReduceValue, None], ReduceElement], ReduceValue
]
ReduceInputQueue = TypedQueue[typing.Union[ReduceElement, None]]
