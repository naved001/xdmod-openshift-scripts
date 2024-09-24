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
from requests.exceptions import ConnectionError
import tempfile
from unittest import TestCase, mock

from openshift_metrics import utils
import os

class TestGetNamespaceAnnotations(TestCase):

    @mock.patch('openshift_metrics.utils.requests.post')
    @mock.patch('openshift_metrics.utils.requests.session')
    def test_get_namespace_attributes(self, mock_session, mock_post):
        mock_response_json = [
            {
                "attributes": {
                    "Allocated Project Name": "Project 1",
                    "Institution-Specific Code": "123"
                },
                "project": {
                    "pi": "PI 1",
                    "id": "1"
                }
            },
            {
                "attributes": {
                    "Allocated Project Name": "Project 2",
                    "Institution-Specific Code": "456"
                },
                "project": {
                    "pi": "PI 2",
                    "id": "2"
                }
            }
        ]
        mock_response = mock.Mock()
        mock_response.json.return_value = mock_response_json

        mock_session_instance = mock_session.return_value
        mock_session_instance.get.return_value = mock_response

        with mock.patch.dict(os.environ, {"CLIENT_ID": "your_client_id", "CLIENT_SECRET": "your_client_secret"}):
            namespaces_dict = utils.get_namespace_attributes()


        expected_namespaces_dict = {
            "Project 1": {
                "cf_pi": "PI 1",
                "cf_project_id": "1",
                "institution_code": "123"
            },
            "Project 2": {
                "cf_pi": "PI 2",
                "cf_project_id": "2",
                "institution_code": "456"
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
            "namespace1+pod1": {
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
            "namespace1+pod2": {
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
        self.assertEqual(output_dict, expected_output_dict)

    def test_merge_metrics_not_empty(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "cpu",
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
                    "namespace": "namespace1",
                    "resource": "cpu",
                },
                "values": [
                    [60, 300],
                ]
            }
        ]
        output_dict = {
            "namespace1+pod1": {
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
            "namespace1+pod2": {
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
            "namespace1+pod1": {
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
            "namespace1+pod2": {
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
            "namespace1+pod1": {
                "namespace": "namespace1",
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

    def test_merge_metrics_same_pod_name(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "podA",
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
                    "pod": "podA",
                    "namespace": "namespace2",
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
            "namespace1+podA": {
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
            "namespace2+podA": {
                "namespace": "namespace2",
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

    def test_merge_metrics_not_empty_with_gpu(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "nvidia.com/gpu",
                    "label_nvidia_com_gpu_product": "Tesla-V100-PCIE-32GB"
                },
                "values": [
                    [0, 1],
                    [60, 1],
                    [120, 2],
                ]
            },
        ]
        output_dict = {
            "namespace1+pod1": {
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
        }
        expected_output_dict = {
            "namespace1+pod1": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu": 10,
                        "gpu_request": 1,
                        "gpu_type": "Tesla-V100-PCIE-32GB",
                        "gpu_resource": "nvidia.com/gpu",
                    },
                    60: {
                        "cpu": 15,
                        "gpu_request": 1,
                        "gpu_type": "Tesla-V100-PCIE-32GB",
                        "gpu_resource": "nvidia.com/gpu",
                    },
                    120: {
                        "cpu": 20,
                        "gpu_request": 2,
                        "gpu_type": "Tesla-V100-PCIE-32GB",
                        "gpu_resource": "nvidia.com/gpu",
                    },
                }
            },
        }
        utils.merge_metrics('gpu_request', test_metric_list, output_dict)
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
                    900: {
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
                    900: {
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
                        "duration": 1800
                    }
                }
            },
            "pod2": {
                "metrics": {
                    0: {
                        "cpu": 2,
                        "mem": 256,
                        "duration": 1800
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
                    900: {
                        "cpu": 20,
                        "mem": 25,
                    },
                    1800: {
                        "cpu": 25,
                        "mem": 25,
                    },
                    2700: {
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
                        "duration": 1800
                    },
                    1800: {
                        "cpu": 25,
                        "mem": 25,
                        "duration": 900
                    },
                    2700: {
                        "cpu": 20,
                        "mem": 25,
                        "duration": 900
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
                    900: {
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
                        "duration": 1800
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEqual(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_with_timeskips(self):
        test_input_dict = {
            "pod1": {
                "metrics": {
                    0: {
                        "cpu": 1,
                        "mem": 4,
                    },
                    900: {
                        "cpu": 1,
                        "mem": 4,
                    },
                    1800: {
                        "cpu": 1,
                        "mem": 4,
                    },
                    5400: { # time skipped
                        "cpu": 1,
                        "mem": 4,
                    },
                    6300: {
                        "cpu": 1,
                        "mem": 4,
                    },
                    8100: { # metric changed and time skipped
                        "cpu": 2,
                        "mem": 8,
                    },
                    9000: {
                        "cpu": 2,
                        "mem": 8,
                    },
                }
            },
            "pod2": {
                "metrics": {
                    0: {
                        "cpu": 2,
                        "mem": 16,
                    },
                    900: {
                        "cpu": 2,
                        "mem": 16,
                    }
                }
            },
        }
        expected_condensed_dict = {
            "pod1": {
                "metrics": {
                    0: {
                        "cpu": 1,
                        "mem": 4,
                        "duration": 2700
                    },
                    5400: {
                        "cpu": 1,
                        "mem": 4,
                        "duration": 1800
                    },
                    8100: {
                        "cpu": 2,
                        "mem": 8,
                        "duration": 1800
                    },
                }
            },
            "pod2": {
                "metrics": {
                    0: {
                        "cpu": 2,
                        "mem": 16,
                        "duration": 1800
                    }
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem'])
        self.assertEqual(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_with_changing_gpu(self):
        test_input_dict = {
            "pod1": {
                "metrics": {
                    0: {
                        "cpu": 1,
                        "mem": 4,
                    },
                    900: {
                        "cpu": 1,
                        "mem": 4,
                    },
                    1800: { # pod acquires a GPU
                        "cpu": 1,
                        "mem": 4,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_V100,
                    },
                    2700: {
                        "cpu": 1,
                        "mem": 4,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_V100,
                    },
                    3600: { # type of GPU is changed
                        "cpu": 1,
                        "mem": 4,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_A100_SXM4,
                    },
                    4500: {
                        "cpu": 1,
                        "mem": 4,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_A100_SXM4,
                    },
                    5400: {
                        "cpu": 1,
                        "mem": 4,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_A100_SXM4,
                    },
                    6300: { # count of GPU is changed
                        "cpu": 1,
                        "mem": 4,
                        "gpu_request": 3,
                        "gpu_type": utils.GPU_A100_SXM4,
                    },
                    7200: {
                        "cpu": 1,
                        "mem": 4,
                        "gpu_request": 3,
                        "gpu_type": utils.GPU_A100_SXM4,
                    },
                    8100: { # no longer using GPUs
                        "cpu": 1,
                        "mem": 4,
                    },
                }
            },
        }
        expected_condensed_dict = {
            "pod1": {
                "metrics": {
                    0: {
                        "cpu": 1,
                        "mem": 4,
                        "duration": 1800
                    },
                    1800: {
                        "cpu": 1,
                        "mem": 4,
                        "duration": 1800,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_V100,
                    },
                    3600: {
                        "cpu": 1,
                        "mem": 4,
                        "duration": 2700,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_A100_SXM4,
                    },
                    6300: {
                        "cpu": 1,
                        "mem": 4,
                        "duration": 1800,
                        "gpu_request": 3,
                        "gpu_type": utils.GPU_A100_SXM4,
                    },
                    8100: {
                        "cpu": 1,
                        "mem": 4,
                        "duration": 900,
                    },
                }
            },
        }
        condensed_dict = utils.condense_metrics(test_input_dict,['cpu','mem', 'gpu_request', 'gpu_type'])
        self.assertEqual(condensed_dict, expected_condensed_dict)


class TestWriteMetricsByPod(TestCase):

    @mock.patch('openshift_metrics.utils.get_namespace_attributes')
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
                "metrics": {
                    0: {
                        "cpu_request": 10,
                        "memory_request": 1048576,
                        "duration": 120,
                        "node": "wrk-1",
                        "node_model": "Dell",
                    },
                    120: {
                        "cpu_request": 20,
                        "memory_request": 1048576,
                        "duration": 60,
                        "node": "wrk-2",
                        "node_model": "Lenovo"
                    }
                }
            },
            "pod2": {
                "namespace": "namespace1",
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
                "metrics": {
                    0: {
                        "cpu_request": 0.5,
                        "memory_request": 2147483648,
                        "duration": 3600
                    },
                }
            },
        }

        expected_output = ("Namespace,Coldfront_PI Name,Coldfront Project ID ,Pod Start Time,Pod End Time,Duration (Hours),Pod Name,CPU Request,GPU Request,GPU Type,GPU Resource,Node,Node Model,Memory Request (GiB),Determining Resource,SU Type,SU Count\n"
                           "namespace1,PI1,123,1970-01-01T00:00:00,1970-01-01T00:02:00,0.0333,pod1,10,0,,,wrk-1,Dell,0.0010,CPU,OpenShift CPU,10\n"
                           "namespace1,PI1,123,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod1,20,0,,,wrk-2,Lenovo,0.0010,CPU,OpenShift CPU,20\n"
                           "namespace1,PI1,123,1970-01-01T00:00:00,1970-01-01T00:01:00,0.0167,pod2,20,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,20\n"
                           "namespace1,PI1,123,1970-01-01T00:01:00,1970-01-01T00:02:00,0.0167,pod2,25,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,25\n"
                           "namespace1,PI1,123,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod2,20,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,20\n"
                           "namespace2,PI2,456,1970-01-01T00:00:00,1970-01-01T00:03:00,0.0500,pod3,45,0,,,Unknown Node,Unknown Model,0.0977,CPU,OpenShift CPU,45\n"
                           "namespace2,PI2,456,1970-01-01T00:00:00,1970-01-01T01:00:00,1.0000,pod4,0.5,0,,,Unknown Node,Unknown Model,2.0000,CPU,OpenShift CPU,0.5\n")

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_pod(test_metrics_dict, tmp.name)
            self.assertEqual(tmp.read(), expected_output)


class TestWriteMetricsByNamespace(TestCase):

    @mock.patch('openshift_metrics.utils.get_namespace_attributes')
    def test_write_metrics_log(self, mock_gna):
        mock_gna.return_value = {
            'namespace1': {
                'cf_pi': 'PI1',
                'cf_project_id': '123',
                'institution_code': '76'
            },
            'namespace2': {
                'cf_pi': 'PI2',
                'cf_project_id': '456',
            }
        }
        test_metrics_dict = {
            "pod1": {
                "namespace": "namespace1",
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
                "metrics": {
                    0: {
                        "cpu_request": 1,
                        "memory_request": 8 * 2**30,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_A100,
                        "gpu_resource": utils.WHOLE_GPU,
                        "duration": 172700 # little under 48 hours, expect to be rounded up in the output
                    },
                }
            },
            "pod5": {
                "namespace": "namespace2",
                "gpu_type": utils.GPU_A100_SXM4,
                "metrics": {
                    0: {
                        "cpu_request": 24,
                        "memory_request": 8 * 2**30,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_A100_SXM4,
                        "gpu_resource": utils.WHOLE_GPU,
                        "duration": 172800
                    },
                }
            },
        }

        expected_output = ("Invoice Month,Project - Allocation,Project - Allocation ID,Manager (PI),Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost\n"
                            "2023-01,namespace1,namespace1,PI1,,,,76,1128,OpenShift CPU,0.013,14.66\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,96,OpenShift CPU,0.013,1.25\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,48,OpenShift GPUA100,1.803,86.54\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,48,OpenShift GPUA100SXM4,2.078,99.74\n")

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(test_metrics_dict, tmp.name, "2023-01")
            self.assertEqual(tmp.read(), expected_output)


    @mock.patch('openshift_metrics.utils.get_namespace_attributes')
    def test_write_metrics_by_namespace_decimal(self, mock_gna):
        """This tests the inaccurate result we get when using floating
        point instead of decimals.

        If floating points are used then the cost is 0.45499999999999996
        which is then rounded down to 0.45.
        """
        mock_gna.return_value = {
            'namespace1': {
                'cf_pi': 'PI1',
                'cf_project_id': '123',
                'institution_code': '76'
            },
        }

        duration = 35 #hours
        rate = 0.013

        test_metrics_dict = {
            "pod1": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu_request": 1,
                        "memory_request": 4 * 2**30,
                        "duration": 35*3600
                    },
                }
        }}

        cost = round(duration*rate,2)
        self.assertEqual(cost, 0.45)

        expected_output = ("Invoice Month,Project - Allocation,Project - Allocation ID,Manager (PI),Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost\n"
                            "2023-01,namespace1,namespace1,PI1,,,,76,35,OpenShift CPU,0.013,0.46\n")

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(test_metrics_dict, tmp.name, "2023-01")
            self.assertEqual(tmp.read(), expected_output)


class TestGetServiceUnit(TestCase):

    def test_cpu_only(self):
        su_type, su_count, determining_resource = utils.get_service_unit(4, 16, 0, None, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 4)
        self.assertEqual(determining_resource, "CPU")

    def test_known_gpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(24, 74, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_A100_SXM4(self):
        su_type, su_count, determining_resource = utils.get_service_unit(32, 245, 1, utils.GPU_A100_SXM4, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_SXM4_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_high_cpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(50, 96, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 3)
        self.assertEqual(determining_resource, "CPU")

    def test_known_gpu_high_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(24, 100, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 2)
        self.assertEqual(determining_resource, "RAM")

    def test_known_gpu_low_cpu_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(2, 4, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_unknown_gpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(8, 64, 1, "Unknown_GPU_Type", utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_zero_count(self):
        su_type, su_count, determining_resource = utils.get_service_unit(8, 64, 0, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_known_mig_gpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 4, 1, utils.GPU_A100_SXM4, utils.MIG_1G_5GB)
        self.assertEqual(su_type, utils.SU_UNKNOWN_MIG_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_unknown_resource(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 4, 1, utils.GPU_A100, "nvidia.com/mig_20G_500GB")
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_unknown_gpu_known_resource(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 4, 1, "Unknown GPU", utils.MIG_2G_10GB)
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_zero_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 0, 0, None, None)
        self.assertEqual(su_type, utils.SU_UNKNOWN)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "CPU")

    def test_zero_cpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0, 1, 0, None, None)
        self.assertEqual(su_type, utils.SU_UNKNOWN)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "CPU")

    def test_memory_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(8, 64, 0, None, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 16)
        self.assertEqual(determining_resource, "RAM")

    def test_fractional_su_cpu_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.5, 0.5, 0, None, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 0.5)
        self.assertEqual(determining_resource, "CPU")

    def test_fractional_su_memory_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.1, 1, 0, None, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 0.25)
        self.assertEqual(determining_resource, "RAM")

    def test_known_gpu_fractional_cpu_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.8, 0.8, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_decimal_return_type(self):
        from decimal import Decimal
        _, su_count, _ = utils.get_service_unit(Decimal("1"), Decimal("8.1"), Decimal("0"), None, None)
        self.assertIsInstance(su_count, Decimal)
        self.assertEqual(su_count, Decimal('2.025'))

    def test_not_decimal_return_type_when_gpu_su_type(self):
        from decimal import Decimal
        su_type, su_count, _ = utils.get_service_unit(Decimal("1"), Decimal("76"), Decimal("1"), utils.GPU_A100, utils.WHOLE_GPU)
        # for GPU SUs, we always round up to the nearest integer
        self.assertIsInstance(su_count, int)
        self.assertEqual(su_count, 2)
        self.assertEqual(su_type, utils.SU_A100_GPU)
