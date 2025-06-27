import typing

from simplemapreduce import MapProcessing, ReduceProcessing, types


class Job(
    typing.Generic[types.MapInputElement, types.MappedInputElement, types.ReduceElement]
):
    """Map reduce job abstraction that handles parallel processing of data.

    This class implements a MapReduce pattern where input elements are processed in parallel
    using a map function and then combined using a reduce function.

    Attributes:
        in_q: Input queue for receiving elements to be processed
        out_q: Output queue for intermediate results between map and reduce stages
        mapper: MapProcessing instance that handles parallel mapping of input elements
        reducer: ReduceProcessing instance that handles reduction of mapped elements

    Type Parameters:
        T: Type of the input elements
        M: Type of the mapped elements (output of map_fn)
        R: Type of the reduced elements (output of reduce_fn)
    """

    def __init__(
        self,
        batch_size: int,
        max_workers: int,
        map_fn: types.MapFnCallable,
        reduce_fn: types.ReduceFnCallable,
    ) -> None:
        """Initialize a new MapReduce job.

        Args:
            batch_size: Number of elements to process in each batch
            max_workers: Maximum number of parallel worker processes
            map_fn: Function to apply to each input element
            reduce_fn: Function to combine mapped elements
        """
        self.in_q: types.MapInputQueue = types.TypedQueue()
        self.out_q: types.MapOutputQueue = types.TypedQueue()
        self.mapper = MapProcessing(
            self.in_q, self.out_q, map_fn, batch_size, max_workers
        )
        self.reducer = ReduceProcessing(self.out_q, reduce_fn)
        self.started = False

    def start(self) -> None:
        """Start the MapReduce job.

        Initializes both the mapping and reducing processes. After calling this method,
        the job will begin processing elements as they are added.
        """
        self.mapper.start()
        self.reducer.start()
        self.started = True

    def add_element(self, element: types.MapInputElement) -> None:
        """Add a new element to be processed by the job.

        Args:
            element: The input element to be processed
        """
        if not self.started:
            self.start()
        self.in_q.put(element)

    def wait(self) -> None:
        """Signal the completion of input data.

        Puts a None sentinel value into the input queue to indicate that no more
        elements will be added. This allows the job to properly terminate once
        all elements have been processed.
        """
        self.in_q.put(None)
        self.mapper.join()
        self.reducer.join()

    def result(self) -> typing.Union[types.ReduceElement, None]:
        """Get the result of the MapReduce job."""
        return self.reducer.return_value
