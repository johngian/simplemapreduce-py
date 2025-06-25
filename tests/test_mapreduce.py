import multiprocessing

import pytest

from simplemapreduce import run_mapreduce
from simplemapreduce.executors import MapProcessing, ReduceProcessing


@pytest.fixture(
    params=[(10, 1, 2), (100, 2, 10), (1000, 2, 100), (10000, 4, 1000), (100, 2, 200)],
    ids=["small", "medium", "large", "xlarge", "batch-size-gt-size"],
)
def map_fixture(request):
    """Fixture with combination of input size, batch size, max workers"""
    (size, max_workers, batch) = request.param
    return ["foo" for _ in range(size)], max_workers, batch


def map_fn(elem):
    """Example map function: Prefix string"""
    return f"mapped-{elem}"


def reduce_fn(accum, elem):
    """Example reduce function: total length of the elements of an iterable"""
    result = len(elem)
    if accum is None:
        return result
    return accum + result


def test_mapreduce(map_fixture):
    """Assert that the result of the map reduce operation is correct"""
    (map_elems, workers, batch_size) = map_fixture
    in_q = multiprocessing.Queue()
    out_q = multiprocessing.Queue()
    mapper = MapProcessing(in_q, out_q, map_fn, batch_size, workers)
    reducer = ReduceProcessing(out_q, reduce_fn)
    mapper.start()
    reducer.start()

    for elem in map_elems:
        in_q.put(elem)
    in_q.put(None)

    mapper.join()
    reducer.join()

    assert reducer.return_value == len("mapped-foo") * len(map_elems)


def test_mapreducehelper(map_fixture):
    """Assert that the result of the map reduce helper is correct"""
    (map_elems, workers, batch_size) = map_fixture
    in_q = multiprocessing.Queue()
    out_q = multiprocessing.Queue()
    for elem in map_elems:
        in_q.put(elem)
    in_q.put(None)
    result = run_mapreduce(in_q, out_q, map_fn, reduce_fn, batch_size, workers)
    assert result == len("mapped-foo") * len(map_elems)
