import multiprocessing

import pytest

from simplemapreduce.executors import MapProcessing


@pytest.fixture(
    params=[(10, 1, 2), (100, 2, 10), (1000, 2, 100), (10000, 4, 1000), (100, 2, 200)],
    ids=["small", "medium", "large", "xlarge", "batch-size-gt-size"],
)
def map_fixture(request):
    """Fixture with combination of input size, batch size, max workers"""
    (size, workers, batch) = request.param
    return [f"Foo-{i}" for i in range(size)], workers, batch


def map_fn(elem):
    """Example map function: Prefix string"""
    return f"Mapped-{elem}"


def test_map(map_fixture):
    """Assert that the result of the map operation is correct"""
    (map_elems, workers, batch_size) = map_fixture
    in_q = multiprocessing.Queue()
    out_q = multiprocessing.Queue()
    out = []
    thread = MapProcessing(in_q, out_q, map_fn, batch_size, workers)
    thread.start()

    for elem in map_elems:
        in_q.put(elem)
    in_q.put(None)

    for _ in range(len(map_elems)):
        out.append(out_q.get())

    thread.join()
    assert out == [f"Mapped-{elem}" for elem in map_elems]
