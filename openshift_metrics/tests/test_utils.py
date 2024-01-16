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

import tempfile
from unittest import TestCase, mock

from openshift_metrics import utils
import openshift as oc


class TestQueryMetric(TestCase):

    @mock.patch('requests.get')
    @mock.patch('time.sleep')
    def test_query_metric(self, mock_sleep, mock_get):
        mock_response = mock.Mock(status_code=200)
        mock_response.json.return_value = {"data": {
            "result": "this is data"
        }}
        mock_get.return_value = mock_response

        metrics = utils.query_metric('fake-url', 'fake-token', 'fake-metric', '2022-03-14', '2022-03-14')
        self.assertEqual(metrics, "this is data")
        self.assertEqual(mock_get.call_count, 1)

    @mock.patch('requests.get')
    @mock.patch('time.sleep')
    def test_query_metric_exception(self, mock_sleep, mock_get):
        mock_get.return_value = mock.Mock(status_code=404)

        self.assertRaises(Exception, utils.query_metric, 'fake-url', 'fake-token',
                          'fake-metric', '2022-03-14', '2022-03-14')
        self.assertEqual(mock_get.call_count, 3)

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
        self.assertEqual(namespaces_dict, expected_namespaces_dict)


class TestMergeMetrics(TestCase):

    def test_merge_metrics_empty(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "cpu",
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
                    "namespace": "namespace1",
                    "resource": "cpu",
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
                "gpu_type": utils.NO_GPU,
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
                "gpu_type": utils.NO_GPU,
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
        self.assertEqual(output_dict, expected_output_dict)

    def test_merge_metrics_not_empty(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
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
                "gpu_type": utils.NO_GPU,
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
                "gpu_type": utils.NO_GPU,
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
                "gpu_type": utils.NO_GPU,
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
                "gpu_type": utils.NO_GPU,
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
        self.assertEqual(output_dict, expected_output_dict)

    def test_merge_metrics_overlapping_range(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "cpu",
                },
                "values": [
                    [0, 10],
                    [60, 10],
                    [120, 10],
                ]
            },

        ]
        test_metric_list_2 = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "cpu",
                },
                "values": [
                    [60, 8],
                    [120, 8],
                    [180, 10],
                ]
            },

        ]
        expected_output_dict = {
            "pod1": {
                "namespace": "namespace1",
                "gpu_type": utils.NO_GPU,
                "metrics": {
                    0: {
                        "cpu": 10
                    },
                    60: {
                        "cpu": 8
                    },
                    120: {
                        "cpu": 8
                    },
                    180: {
                        "cpu": 10
                    },
                }
            },
        }
        output_dict = {}
        utils.merge_metrics('cpu', test_metric_list, output_dict)
        utils.merge_metrics('cpu', test_metric_list_2, output_dict)
        self.assertEqual(output_dict, expected_output_dict)

        # trying to merge the same metrics again should not change anything
        utils.merge_metrics('cpu', test_metric_list_2, output_dict)
        self.assertEqual(output_dict, expected_output_dict)

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
            "pod2": {
                "metrics": {
                    0: {
                        "cpu": 2,
                        "mem": 256,
                    },
                    100: {
                        "cpu": 2,
                        "mem": 256,
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
                        "duration": 120
                    }
                }
            },
            "pod2": {
                "metrics": {
                    0: {
                        "cpu": 2,
                        "mem": 256,
                        "duration": 200
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEqual(condensed_dict, expected_condensed_dict)


    def test_condense_metrics_no_interval(self):
        test_input_dict = {
            "pod1": {
                "metrics": {
                    0: {
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
                        "duration": 900
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEqual(condensed_dict, expected_condensed_dict)

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
                        "duration": 120
                    },
                    120: {
                        "cpu": 25,
                        "mem": 25,
                        "duration": 60
                    },
                    180: {
                        "cpu": 20,
                        "mem": 25,
                        "duration": 60
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEqual(condensed_dict, expected_condensed_dict)

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
                        "duration": 120
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEqual(condensed_dict, expected_condensed_dict)

class TestWriteMetricsByPod(TestCase):

    @mock.patch('openshift_metrics.utils.get_namespace_annotations')
    def test_write_metrics_log(self, mock_gna):
        mock_gna.return_value = {
            'namespace1': {
                'cf_pi': 'PI1',
                'cf_project_id': '123',
            },
            'namespace2': {
                'cf_pi': 'PI2',
                'cf_project_id': '456',
            }
        }
        test_metrics_dict = {
            "pod1": {
                "namespace": "namespace1",
                "gpu_type": utils.NO_GPU,
                "metrics": {
                    0: {
                        "cpu_request": 10,
                        "memory_request": 1048576,
                        "duration": 120
                    },
                    120: {
                        "cpu_request": 20,
                        "memory_request": 1048576,
                        "duration": 60
                    }
                }
            },
            "pod2": {
                "namespace": "namespace1",
                "gpu_type": utils.NO_GPU,
                "metrics": {
                    0: {
                        "cpu_request": 20,
                        "memory_request": 10485760,
                        "duration": 60
                    },
                    60: {
                        "cpu_request": 25,
                        "memory_request": 10485760,
                        "duration": 60
                    },
                    120: {
                        "cpu_request": 20,
                        "memory_request": 10485760,
                        "duration": 60
                    }
                }
            },
            "pod3": {
                "namespace": "namespace2",
                "gpu_type": utils.NO_GPU,
                "metrics": {
                    0: {
                        "cpu_request": 45,
                        "memory_request": 104857600,
                        "duration": 180
                    },
                }
            },
            "pod4": { # this results in 0.5 SU
                "namespace": "namespace2",
                "gpu_type": utils.NO_GPU,
                "metrics": {
                    0: {
                        "cpu_request": 0.5,
                        "memory_request": 2147483648,
                        "duration": 3600
                    },
                }
            },
        }

        expected_output = ("Namespace,Coldfront_PI Name,Coldfront Project ID ,Pod Start Time,Pod End Time,Duration (Hours),Pod Name,CPU Request,GPU Request,GPU Type,Memory Request (GiB),Determining Resource,SU Type,SU Count\n"
                           "namespace1,PI1,123,1970-01-01T00:00:00,1970-01-01T00:02:00,0.0333,pod1,10,0,No GPU,0.001,CPU,OpenShift CPU,10.0\n"
                           "namespace1,PI1,123,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod1,20,0,No GPU,0.001,CPU,OpenShift CPU,20.0\n"
                           "namespace1,PI1,123,1970-01-01T00:00:00,1970-01-01T00:01:00,0.0167,pod2,20,0,No GPU,0.0098,CPU,OpenShift CPU,20.0\n"
                           "namespace1,PI1,123,1970-01-01T00:01:00,1970-01-01T00:02:00,0.0167,pod2,25,0,No GPU,0.0098,CPU,OpenShift CPU,25.0\n"
                           "namespace1,PI1,123,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod2,20,0,No GPU,0.0098,CPU,OpenShift CPU,20.0\n"
                           "namespace2,PI2,456,1970-01-01T00:00:00,1970-01-01T00:03:00,0.05,pod3,45,0,No GPU,0.0977,CPU,OpenShift CPU,45.0\n"
                           "namespace2,PI2,456,1970-01-01T00:00:00,1970-01-01T01:00:00,1.0,pod4,0.5,0,No GPU,2.0,CPU,OpenShift CPU,0.5\n")

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_pod(test_metrics_dict, tmp.name)
            self.assertEqual(tmp.read(), expected_output)


class TestWriteMetricsByNamespace(TestCase):

    @mock.patch('openshift_metrics.utils.get_namespace_annotations')
    def test_write_metrics_log(self, mock_gna):
        mock_gna.return_value = {
            'namespace1': {
                'cf_pi': 'PI1',
                'cf_project_id': '123',
            },
            'namespace2': {
                'cf_pi': 'PI2',
                'cf_project_id': '456',
            }
        }
        test_metrics_dict = {
            "pod1": {
                "namespace": "namespace1",
                "gpu_type": utils.NO_GPU,
                "metrics": {
                    0: {
                        "cpu_request": 2,
                        "memory_request": 4 * 2**30,
                        "duration": 43200
                    },
                    43200: {
                        "cpu_request": 4,
                        "memory_request": 4 * 2**30,
                        "duration": 43200
                    }
                }
            },
            "pod2": {
                "namespace": "namespace1",
                "gpu_type": utils.NO_GPU,
                "metrics": {
                    0: {
                        "cpu_request": 4,
                        "memory_request": 1 * 2**30,
                        "duration": 86400
                    },
                    86400: {
                        "cpu_request": 20,
                        "memory_request": 1 * 2**30,
                        "duration": 172800
                    }
                }
            },
            "pod3": {
                "namespace": "namespace2",
                "gpu_type": utils.NO_GPU,
                "metrics": {
                    0: {
                        "cpu_request": 1,
                        "memory_request": 8 * 2**30,
                        "duration": 172800
                    },
                }
            },
            "pod4": {
                "namespace": "namespace2",
                "gpu_type": utils.GPU_A100,
                "metrics": {
                    0: {
                        "cpu_request": 1,
                        "memory_request": 8 * 2**30,
                        "gpu_request": 1,
                        "duration": 172700 # little under 48 hours, expect to be rounded up in the output
                    },
                }
            },
            "pod5": {
                "namespace": "namespace2",
                "gpu_type": utils.GPU_A2,
                "metrics": {
                    0: {
                        "cpu_request": 24,
                        "memory_request": 8 * 2**30,
                        "gpu_request": 1,
                        "duration": 172800
                    },
                }
            },
        }

        expected_output = ("Invoice Month,Project - Allocation,Project - Allocation ID,Manager (PI),Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost\n"
                            "2023-01,namespace1,namespace1,PI1,,,,,1128,OpenShift CPU,0.013,14.66\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,96,OpenShift CPU,0.013,1.25\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,48,OpenShift GPUA100,1.803,86.54\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,144,OpenShift GPUA2,0.466,67.1\n")

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(test_metrics_dict, tmp.name, "2023-01")
            self.assertEqual(tmp.read(), expected_output)


class TestGetServiceUnit(TestCase):

    def test_cpu_only(self):
        su_type, su_count, determining_resource = utils.get_service_unit(4, 16, 0, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 4)
        self.assertEqual(determining_resource, "CPU")

    def test_known_gpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(24, 96, 1, utils.GPU_A100)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_high_cpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(50, 96, 1, utils.GPU_A100)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 3)
        self.assertEqual(determining_resource, "CPU")

    def test_known_gpu_high_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(24, 100, 1, utils.GPU_A100)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 2)
        self.assertEqual(determining_resource, "RAM")

    def test_known_gpu_low_cpu_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(2, 4, 1, utils.GPU_A100)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_unknown_gpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(8, 64, 1, "Unknown_GPU_Type")
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_zero_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 0, 0, None)
        self.assertEqual(su_type, utils.SU_UNKNOWN)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "CPU")

    def test_zero_cpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0, 1, 0, None)
        self.assertEqual(su_type, utils.SU_UNKNOWN)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "CPU")

    def test_memory_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(8, 64, 0, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 16)
        self.assertEqual(determining_resource, "RAM")

    def test_fractional_su_cpu_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.5, 0.5, 0, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 0.5)
        self.assertEqual(determining_resource, "CPU")

    def test_fractional_su_memory_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.1, 1, 0, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 0.25)
        self.assertEqual(determining_resource, "RAM")

    def test_known_gpu_fractional_cpu_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.8, 0.8, 1, utils.GPU_A100)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")
