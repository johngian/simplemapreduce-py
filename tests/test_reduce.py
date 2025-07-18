import pytest

from simplemapreduce.executors import ReduceProcessing
from simplemapreduce.metrics import Metrics
from simplemapreduce.types import TypedQueue


@pytest.fixture(
    params=[10, 100, 1000, 10000], ids=["small", "medium", "large", "xlarge"]
)
def reduce_input(request):
    """Fixture with a combination of input size, batch size, max workers"""
    size = request.param
    return [i for i in range(size)]


def reduce_fn(accum, elem):
    """Example of a reduce function: Calculate the sum of two elements"""
    if accum is None:
        return elem
    return accum + elem


def test_reduce(reduce_input):
    """Assert that the result of the reduce operation is correct"""
    in_q = TypedQueue()
    metrics = Metrics()
    thread = ReduceProcessing(in_q, reduce_fn, metrics)
    thread.start()

    for elem in reduce_input:
        in_q.put(elem)
    in_q.put(None)

    thread.join()
    assert thread.return_value == sum(reduce_input)
