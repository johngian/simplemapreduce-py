import datetime
import time

from simplemapreduce.metrics import Metrics


def test_element_added():
    """Assert that the element_added method increments the elements_added counter"""
    metrics = Metrics()
    for _ in range(10):
        metrics.element_added()
    assert metrics.elements_added == 10


def test_element_mapped():
    """Assert that the element_mapped method increments the elements_mapped counter"""
    metrics = Metrics()
    for _ in range(10):
        metrics.element_mapped()
    assert metrics.elements_mapped == 10


def test_element_reduced():
    """Assert that the element_reduced method increments the elements_reduced counter"""
    metrics = Metrics()
    for _ in range(10):
        metrics.element_reduced()
    assert metrics.elements_reduced == 10


def test_time_elapsed():
    """Assert that the time_elapsed method returns the time elapsed"""
    metrics = Metrics()
    metrics.job_started()
    time.sleep(3)
    assert metrics.time_elapsed() >= datetime.timedelta(seconds=3)
