import functools
import time

import pytest

from simplemapreduce import run_mapreduce
from simplemapreduce.types import TypedQueue


def map_func(sleep_time, x):
    time.sleep(sleep_time)
    return x


def reduce_func(sleep_time, accum, x):
    time.sleep(sleep_time)
    if accum is None:
        return x
    return accum + x


def run_single_threaded(elems, sleep_time):
    start_time = time.perf_counter_ns()
    mapped_elems = map(functools.partial(map_func, sleep_time), elems)
    value = functools.reduce(functools.partial(reduce_func, sleep_time), mapped_elems)
    end_time = time.perf_counter_ns()
    elapsed = end_time - start_time
    return value, elapsed


def run_multi_threaded(elems, sleep_time, batch_size, max_workers):
    in_q = TypedQueue()
    out_q = TypedQueue()
    for elem in elems:
        in_q.put(elem)
    in_q.put(None)
    start_time = time.perf_counter_ns()
    value = run_mapreduce(
        in_q,
        out_q,
        functools.partial(map_func, sleep_time),
        functools.partial(reduce_func, sleep_time),
        batch_size,
        max_workers,
    )
    end_time = time.perf_counter_ns()
    elapsed = end_time - start_time
    return value, elapsed


@pytest.mark.parametrize("sleep_time", [0.001, 0.01])
@pytest.mark.parametrize("input_size", [100, 1000])
@pytest.mark.parametrize("batch_size", [10])
@pytest.mark.parametrize("max_workers", [4])
@pytest.mark.benchmark
def test_benchmark_wait_heavy(sleep_time, input_size, batch_size, max_workers):
    """Benchmark between multithreaded and singlethreaded map/reduce operation with wait time"""
    map_elems = [i for i in range(input_size)]
    (serial_result, serial_time) = run_single_threaded(map_elems, sleep_time)
    (concurrent_result, concurrent_time) = run_multi_threaded(
        map_elems, sleep_time, batch_size, max_workers
    )

    assert serial_result == concurrent_result
    assert 100 * ((concurrent_time - serial_time) / serial_time) < 100


@pytest.mark.parametrize("sleep_time", [0])
@pytest.mark.parametrize("input_size", [100, 1000])
@pytest.mark.parametrize("batch_size", [10])
@pytest.mark.parametrize("max_workers", [4])
@pytest.mark.benchmark
def test_benchmark_no_wait(sleep_time, input_size, batch_size, max_workers):
    """Benchmark between multithreaded and singlethreaded map/reduce operation with no wait time"""
    map_elems = [i for i in range(input_size)]
    (serial_result, serial_time) = run_single_threaded(map_elems, sleep_time)
    (concurrent_result, concurrent_time) = run_multi_threaded(
        map_elems, sleep_time, batch_size, max_workers
    )

    assert serial_result == concurrent_result
    assert 100 * ((concurrent_time - serial_time) / serial_time) > 100
