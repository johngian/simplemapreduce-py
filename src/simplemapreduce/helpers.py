import simplemapreduce.types
from simplemapreduce.executors import MapProcessing, ReduceProcessing
from simplemapreduce.metrics import Metrics


def run_mapreduce(
    in_q: simplemapreduce.types.MapInputQueue,
    out_q: simplemapreduce.types.MapOutputQueue,
    map_fn: simplemapreduce.types.MapFnCallable,
    reduce_fn: simplemapreduce.types.ReduceFnCallable,
    batch_size: int,
    max_workers: int,
):
    """Helper function to run a map/reduce job"""
    metrics = Metrics()
    mapper = MapProcessing(in_q, out_q, map_fn, batch_size, max_workers, metrics)
    reducer = ReduceProcessing(out_q, reduce_fn, metrics)
    mapper.start()
    reducer.start()
    mapper.join()
    reducer.join()
    return reducer.return_value
