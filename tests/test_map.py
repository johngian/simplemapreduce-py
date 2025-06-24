import multiprocessing

import pytest

from simplemapreduce import MapExecutor


@pytest.fixture(
    params=[10, 100, 1000, 10000], ids=["small", "medium", "large", "xlarge"]
)
def map_input(request):
    size = request.param
    return [f"Foo-{i}" for i in range(size)]


def map_fn(elem):
    return f"Mapped-{elem}"


def test_map(map_input):
    in_q = multiprocessing.Queue()
    out_q = multiprocessing.Queue()
    out = []
    thread = MapExecutor(in_q, out_q, map_fn, max_workers=1)
    thread.start()

    for elem in map_input:
        in_q.put(elem)
    in_q.put(None)

    for _ in range(len(map_input)):
        out.append(out_q.get())

    thread.join()
    assert out == [f"Mapped-{elem}" for elem in map_input]
