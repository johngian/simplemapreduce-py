import multiprocessing

import pytest

from simplemapreduce.executors import ReduceExecutor


@pytest.fixture(
    params=[10, 100, 1000, 10000], ids=["small", "medium", "large", "xlarge"]
)
def map_input(request):
    size = request.param
    return [i for i in range(size)]


def reduce_fn(accum, elem):
    if accum is None:
        return elem
    return accum + elem


def test_map(map_input):
    in_q = multiprocessing.Queue()
    thread = ReduceExecutor(in_q, reduce_fn)
    thread.start()

    for elem in map_input:
        in_q.put(elem)
    in_q.put(None)

    thread.join()
    assert thread.return_value == sum(map_input)
