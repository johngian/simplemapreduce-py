import json
import os
import signal
from contextlib import redirect_stdout
from io import StringIO

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


def test_mapreduce_metrics(map_fixture):
    """Assert that the result of the map reduce metrics are correct"""
    (map_elems, workers, batch_size) = map_fixture
    job = Job(batch_size, workers, map_fn, reduce_fn)
    for elem in map_elems:
        job.add_element(elem)
    job.wait()

    assert job.metrics.elements_mapped == len(map_elems)
    assert job.metrics.elements_reduced == len(map_elems)
    assert job.metrics.time_elapsed().total_seconds() > 0


def test_signal_handler(map_fixture):
    """Assert that the signal handler prints the metrics to stdout"""
    captured_output = StringIO()
    with redirect_stdout(captured_output):
        (map_elems, workers, batch_size) = map_fixture
        job = Job(batch_size, workers, map_fn, reduce_fn)
        for elem in map_elems:
            job.add_element(elem)
        job.wait()
        os.kill(os.getpid(), signal.SIGUSR1)

    out = captured_output.getvalue()
    assert len(out.strip()) > 0
    assert set(json.loads(out).keys()) == {
        "job_name",
        "elements_added",
        "elements_mapped",
        "time_elapsed",
    }
