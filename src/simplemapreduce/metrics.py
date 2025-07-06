import json
import threading
from datetime import datetime, timedelta


class Metrics:
    def __init__(self):
        self.elements_added = 0
        self.elements_mapped = 0
        self.elements_reduced = 0
        self.start_time = None
        self.sync_lock = threading.Lock()

    def element_added(self):
        with self.sync_lock:
            self.elements_added += 1

    def element_mapped(self):
        with self.sync_lock:
            self.elements_mapped += 1

    def element_reduced(self):
        with self.sync_lock:
            self.elements_reduced += 1

    def job_started(self):
        self.start_time = datetime.now()

    def time_elapsed(self) -> timedelta:
        return datetime.now() - self.start_time

    def __str__(self):
        return json.dumps(self)

    def to_dict(self):
        metrics = {
            "elements_added": self.elements_added,
            "elements_mapped": self.elements_mapped,
            "time_elapsed": self.time_elapsed().total_seconds(),
        }
        return metrics
