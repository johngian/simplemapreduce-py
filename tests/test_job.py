import pytest

from simplemapreduce.jobs import Job


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


def test_mapreduce_job(map_fixture):
    """Assert that the result of the map reduce operation is correct"""
    (map_elems, workers, batch_size) = map_fixture
    job = Job(batch_size, workers, map_fn, reduce_fn)

    for elem in map_elems:
        job.add_element(elem)
    job.wait()

    assert job.result() == len("mapped-foo") * len(map_elems)
