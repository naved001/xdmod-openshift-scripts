from unittest import TestCase
from datetime import datetime
from decimal import Decimal

from openshift_metrics import invoice


class TestPodGetRuntime(TestCase):
    def setUp(self):
        """Gives us a pod that starts at 2024-10-11 12:00 UTC and ends at 2024-10-11 20:00 UTC"""
        self.pod = invoice.Pod(
            pod_name="test-pod",
            namespace="test-namespace",
            start_time=int(datetime(2024, 10, 11, 12, 0).timestamp()),
            duration=3600 * 8,
            cpu_request=Decimal("1.0"),
            gpu_request=Decimal(0),
            memory_request=Decimal("4.0"),
            gpu_type=None,
            gpu_resource=None,
            node_hostname="node-1",
            node_model=None,
        )

    def test_no_ignore_times(self):
        runtime = self.pod.get_runtime()
        self.assertEqual(runtime, Decimal("8.0"))

    def test_one_ignore_range(self):
        ignore_range = [(datetime(2024, 10, 11, 13, 0), datetime(2024, 10, 11, 14, 0))]
        self.assertEqual(self.pod.get_runtime(ignore_range), Decimal(7.0))

    def test_multiple_ignore_times(self):
        ignore_times = [
            (datetime(2024, 10, 11, 13, 0), datetime(2024, 10, 11, 14, 0)),
            (datetime(2024, 10, 11, 14, 0), datetime(2024, 10, 11, 15, 0)),
            (datetime(2024, 10, 11, 19, 0), datetime(2024, 10, 11, 20, 0)),
        ]
        self.assertEqual(self.pod.get_runtime(ignore_times), Decimal(5.0))

    def test_ignore_times_outside_runtime(self):
        ignore_times = [
            (
                datetime(2024, 10, 11, 10, 0),
                datetime(2024, 10, 11, 11, 0),
            ),  # before start
            (datetime(2024, 10, 11, 20, 0), datetime(2024, 10, 11, 22, 0)),  # after end
        ]
        self.assertEqual(self.pod.get_runtime(ignore_times), Decimal(8.0))

    def test_partial_overlap_ignore_range(self):
        ignore_range = [
            (datetime(2024, 10, 11, 10, 30), datetime(2024, 10, 11, 14, 30))
        ]
        self.assertEqual(self.pod.get_runtime(ignore_range), Decimal(5.5))

    def test_ignore_range_greater_than_pod_runtime(self):
        ignore_range = [
            (datetime(2024, 10, 11, 11, 00), datetime(2024, 10, 11, 21, 00))
        ]
        self.assertEqual(self.pod.get_runtime(ignore_range), Decimal(0))

    def test_runtime_is_never_negative(self):
        ignore_times = [
            (datetime(2024, 10, 11, 13, 0), datetime(2024, 10, 11, 17, 0)),
            (datetime(2024, 10, 11, 13, 0), datetime(2024, 10, 11, 17, 0)),
            (datetime(2024, 10, 11, 10, 0), datetime(2024, 10, 11, 22, 0)),
        ]
        self.assertEqual(self.pod.get_runtime(ignore_times), Decimal(0.0))
