import multiprocessing

import pytest

from simplemapreduce.executors import MapExecutor


@pytest.fixture(
    params=[(10, 1), (100, 2), (1000, 2), (10000, 4)],
    ids=["small", "medium", "large", "xlarge"],
)
def map_fixture(request):
    (size, workers) = request.param
    return [f"Foo-{i}" for i in range(size)], workers


def map_fn(elem):
    return f"Mapped-{elem}"


def test_map(map_fixture):
    (map_elems, workers) = map_fixture
    in_q = multiprocessing.Queue()
    out_q = multiprocessing.Queue()
    out = []
    thread = MapExecutor(in_q, out_q, map_fn, max_workers=workers)
    thread.start()

    for elem in map_elems:
        in_q.put(elem)
    in_q.put(None)

    for _ in range(len(map_elems)):
        out.append(out_q.get())

    thread.join()
    assert out == [f"Mapped-{elem}" for elem in map_elems]
