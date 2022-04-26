#   Licensed under the Apache License, Version 2.0 (the "License"); you may
#   not use this file except in compliance with the License. You may obtain
#   a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.
#

import mock
import requests
import tempfile
import time
from unittest import TestCase

from openshift_metrics import utils
import openshift as oc


class TestQueryMetric(TestCase):

    @mock.patch('requests.get')
    def test_query_metric(self, mock_get):
        mock_response = mock.Mock(status_code=200)
        mock_response.json.return_value = {"data": {
            "result": "this is data"
        }}
        mock_get.return_value = mock_response

        metrics = utils.query_metric('fake-url', 'fake-token', 'fake-metric', '2022-03-14')
        self.assertEqual(metrics, "this is data")
        self.assertEqual(mock_get.call_count, 1)

    @mock.patch('requests.get')
    def test_query_metric_exception(self, mock_get):
        mock_get.return_value = mock.Mock(status_code=404)

        self.assertRaises(Exception, utils.query_metric, 'fake-url', 'fake-token',
                          'fake-metric', '2022-03-14')
        self.assertEqual(mock_get.call_count, 3)

    @mock.patch('requests.get')
    def test_query_metric_exception_retry_count(self, mock_get):
        mock_get.return_value = mock.Mock(status_code=404)

        self.assertRaises(Exception, utils.query_metric, 'fake-url', 'fake-token',
                          'fake-metric', '2022-03-14', retry=2)
        self.assertEqual(mock_get.call_count, 2)


class TestGetNamespaceAnnotations(TestCase):

    @mock.patch('openshift.selector')
    def test_get_namespace_annotations(self, mock_selector):
        mock_namespaces = mock.Mock()
        mock_namespaces.objects.return_value = [
            oc.apiobject.APIObject({
                'metadata': {
                    'name': 'namespace1',
                    'annotations': {
                        'anno1': 'value1',
                        'anno2': 'value2'
                    }
                }
            }),
            oc.apiobject.APIObject({
                'metadata': {
                    'name': 'namespace2',
                    'annotations': {
                        'anno3': 'value3',
                        'anno4': 'value4'
                    }
                }
            })
        ]
        mock_selector.return_value = mock_namespaces

        namespaces_dict = utils.get_namespace_annotations()
        expected_namespaces_dict = {
            'namespace1': {
                'anno1': 'value1',
                'anno2': 'value2'
            },
            'namespace2': {
                'anno3': 'value3',
                'anno4': 'value4'
            }
        }
        self.assertEquals(namespaces_dict, expected_namespaces_dict)


class TestMergeMetrics(TestCase):

    def test_merge_metrics_empty(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1"
                },
                "values": [
                    [0, 10],
                    [60, 15],
                    [120, 20],
                ]
            },
            {
                "metric": {
                    "pod": "pod2",
                    "namespace": "namespace1"
                },
                "values": [
                    [0, 30],
                    [60, 35],
                    [120, 40],
                ]
            }
        ]
        expected_output_dict = {
            "pod1": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 10
                    },
                    60: {
                        "cpu": 15
                    },
                    120: {
                        "cpu": 20
                    },
                }
            },
            "pod2": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 30
                    },
                    60: {
                        "cpu": 35
                    },
                    120: {
                        "cpu": 40
                    },
                }
            }
        }
        output_dict = {}
        utils.merge_metrics('cpu', test_metric_list, output_dict)
        self.assertEquals(output_dict, expected_output_dict)

    def test_merge_metrics_not_empty(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1"
                },
                "values": [
                    [0, 100],
                    [60, 150],
                    [120, 200],
                ]
            },
            {
                "metric": {
                    "pod": "pod2",
                    "namespace": "namespace1"
                },
                "values": [
                    [60, 300],
                ]
            }
        ]
        output_dict = {
            "pod1": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 10
                    },
                    60: {
                        "cpu": 15
                    },
                    120: {
                        "cpu": 20
                    },
                }
            },
            "pod2": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 30
                    },
                    60: {
                        "cpu": 35
                    },
                    120: {
                        "cpu": 40
                    },
                }
            }
        }
        expected_output_dict = {
            "pod1": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 10,
                        "mem": 100
                    },
                    60: {
                        "cpu": 15,
                        "mem": 150
                    },
                    120: {
                        "cpu": 20,
                        "mem": 200
                    },
                }
            },
            "pod2": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 30
                    },
                    60: {
                        "cpu": 35,
                        "mem": 300
                    },
                    120: {
                        "cpu": 40
                    },
                }
            }
        }
        utils.merge_metrics('mem', test_metric_list, output_dict)
        self.assertEquals(output_dict, expected_output_dict)


class TestCondenseMetrics(TestCase):

    def test_condense_metrics(self):
        test_input_dict = {
            "pod1": {
                "metrics": {
                    0: {
                        "cpu": 10,
                        "mem": 15,
                    },
                    60: {
                        "cpu": 10,
                        "mem": 15,
                    }
                }
            },
        }
        expected_condensed_dict = {
            "pod1": {
                "metrics": {
                    0: {
                        "cpu": 10,
                        "mem": 15,
                        "duration": 119
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEquals(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_with_change(self):
        test_input_dict = {
            "pod2": {
                "metrics": {
                    0: {
                        "cpu": 20,
                        "mem": 25,
                    },
                    60: {
                        "cpu": 20,
                        "mem": 25,
                    },
                    120: {
                        "cpu": 25,
                        "mem": 25,
                    },
                    180: {
                        "cpu": 20,
                        "mem": 25,
                    }
                }
            },
        }
        expected_condensed_dict = {
            "pod2": {
                "metrics": {
                    0: {
                        "cpu": 20,
                        "mem": 25,
                        "duration": 119
                    },
                    120: {
                        "cpu": 25,
                        "mem": 25,
                        "duration": 59
                    },
                    180: {
                        "cpu": 20,
                        "mem": 25,
                        "duration": 59
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEquals(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_skip_metric(self):
        test_input_dict = {
            "pod3": {
                "metrics": {
                    0: {
                        "cpu": 30,
                        "mem": 35,
                        "gpu": 1,
                    },
                    60: {
                        "cpu": 30,
                        "mem": 35,
                        "gpu": 2,
                    },
                }
            }
        }
        expected_condensed_dict = {
            "pod3": {
                "metrics": {
                    0: {
                        "cpu": 30,
                        "mem": 35,
                        "gpu": 1,
                        "duration": 119
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEquals(condensed_dict, expected_condensed_dict)

class TestWriteMetricsLog(TestCase):

    @mock.patch('openshift_metrics.utils.get_namespace_annotations')
    def test_write_metrics_log(self, mock_gna):
        mock_gna.return_value = {
            'namespace1': {
                'openshift.io/requester': 'PI1',
            },
            'namespace2': {
                'openshift.io/requester': 'PI2',
            }
        }
        test_metrics_dict = {
            "pod1": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 10,
                        "allocated_cpu": 20,
                        "allocated_memory": 1048576,
                        "duration": 119
                    },
                    120: {
                        "cpu": 20,
                        "allocated_cpu": 20,
                        "allocated_memory": 1048576,
                        "duration": 59
                    }
                }
            },
            "pod2": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 20,
                        "allocated_cpu": 30,
                        "allocated_memory": 10485760,
                        "duration": 59
                    },
                    60: {
                        "cpu": 25,
                        "allocated_cpu": 30,
                        "allocated_memory": 10485760,
                        "duration": 59
                    },
                    120: {
                        "cpu": 20,
                        "allocated_cpu": 30,
                        "allocated_memory": 10485760,
                        "duration": 59
                    }
                }
            },
            "pod3": {
                "namespace": "namespace2",
                "metrics": {
                    0: {
                        "cpu": 45,
                        "allocated_cpu": 50,
                        "allocated_memory": 104857600,
                        "duration": 179
                    },
                }
            },
        }

        expected_output = ("0|0|TBD|||PI1|PI1||namespace1||1969-12-31T19:00:00|1969-12-31T19:00:00|1969-12-31T19:00:00|1969-12-31T19:01:59|0-0:01:59||COMPLETED|1|10|20|1.0|cpu=20,mem=1.0|cpu=20,mem=1.0|0-0:01:59||pod1\n"
                           "1|1|TBD|||PI1|PI1||namespace1||1969-12-31T19:02:00|1969-12-31T19:02:00|1969-12-31T19:02:00|1969-12-31T19:02:59|0-0:00:59||COMPLETED|1|20|20|1.0|cpu=20,mem=1.0|cpu=20,mem=1.0|0-0:00:59||pod1\n"
                           "2|2|TBD|||PI1|PI1||namespace1||1969-12-31T19:00:00|1969-12-31T19:00:00|1969-12-31T19:00:00|1969-12-31T19:00:59|0-0:00:59||COMPLETED|1|20|30|10.0|cpu=30,mem=10.0|cpu=30,mem=10.0|0-0:00:59||pod2\n"
                           "3|3|TBD|||PI1|PI1||namespace1||1969-12-31T19:01:00|1969-12-31T19:01:00|1969-12-31T19:01:00|1969-12-31T19:01:59|0-0:00:59||COMPLETED|1|25|30|10.0|cpu=30,mem=10.0|cpu=30,mem=10.0|0-0:00:59||pod2\n"
                           "4|4|TBD|||PI1|PI1||namespace1||1969-12-31T19:02:00|1969-12-31T19:02:00|1969-12-31T19:02:00|1969-12-31T19:02:59|0-0:00:59||COMPLETED|1|20|30|10.0|cpu=30,mem=10.0|cpu=30,mem=10.0|0-0:00:59||pod2\n"
                           "5|5|TBD|||PI2|PI2||namespace2||1969-12-31T19:00:00|1969-12-31T19:00:00|1969-12-31T19:00:00|1969-12-31T19:02:59|0-0:02:59||COMPLETED|1|45|50|100.0|cpu=50,mem=100.0|cpu=50,mem=100.0|0-0:02:59||pod3\n")

        tmp_file_name = "%s/test-metrics-%s.log" % (tempfile.gettempdir(), time.time())
        utils.write_metrics_log(test_metrics_dict, tmp_file_name)
        f = open(tmp_file_name, "r")
        self.assertEquals(f.read(), expected_output)
        f.close()
