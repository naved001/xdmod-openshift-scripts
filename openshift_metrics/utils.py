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

import datetime
import json
import requests
import time
import math

import openshift

DELIMITER = ','

# GPU types
GPU_A100 = "nvidia.com/gpu_A100"
GPU_A10 = "nvidia.com/gpu_A10"
GPU_MOC = "nvidia.com/gpu"
NO_GPU = "No GPU"

# SU Types
SU_CPU = "SU_CPU"
SU_A100_GPU = "SU_A100_GPU"
SU_A10_GPU = "SU_A10_GPU"
SU_MOC_GPU = "SU_MOC_GPU"
SU_UNKNOWN_GPU = "SU_UNKNOWN_GPU"
SU_UNKNOWN = "SU_UNKNOWN"

SU_COST = {
            SU_A100_GPU: 1.92,
            SU_A10_GPU: 0.384,
            SU_CPU: 0.016,
            SU_MOC_GPU: 0.512,
            SU_UNKNOWN_GPU: 0.768,
            SU_UNKNOWN: 0.0,
}

class EmptyResultError(Exception):
    """Raise when no results are retrieved for a query"""
    pass

def query_metric(openshift_url, token, metric, report_start_date, report_end_date, disable_ssl=False,
                 retry=3):
    attempt = 0
    data = None
    headers = {'Authorization': "Bearer %s" % token}
    day_url_vars = "start=%sT00:00:00Z&end=%sT23:59:59Z" % (report_start_date, report_end_date)
    print("Retrieving metric: %s" % metric)
    for attempt in range(retry):
        url = "%s/api/v1/query_range?query=%s&%s&step=60s" % (openshift_url, metric, day_url_vars)
        r = requests.get(url, headers=headers, verify=(not disable_ssl))
        if r.status_code != 200:
            print("%s Response: %s" % (r.status_code, r.reason))
        else:
            data = r.json()['data']['result']
            if data:
                break
            else:
                print("Empty result set")
        time.sleep(3)
    if not data:
        raise EmptyResultError('Error retrieving metric: %s' % metric)
    return data

def get_namespace_annotations():
    namespaces_dict = {}
    namespaces = openshift.selector("namespaces").objects()
    for namespace in namespaces:
        namespace_dict = namespace.as_dict()['metadata']
        namespaces_dict[namespace_dict['name']] = namespace_dict['annotations']
    return namespaces_dict

def get_date_chunks(end_date, report_length):
    date_format = '%Y-%m-%d'
    end_date = datetime.datetime.strptime(end_date, date_format)
    start_date = end_date - datetime.timedelta(days=report_length)

    date_chunks = []
    while start_date <= end_date:
        chunk_end_date = start_date + datetime.timedelta(days=6)
        if chunk_end_date > end_date:
            chunk_end_date = end_date
        date_chunks.append((start_date.strftime(date_format), chunk_end_date.strftime(date_format)))
        start_date += datetime.timedelta(days=7)

    return date_chunks


def get_service_unit(cpu_count, memory_count, gpu_count, gpu_type):
    su_type = SU_UNKNOWN
    su_count = 0
    if gpu_type == NO_GPU:
        gpu_type = None

    # pods that requested a specific GPU but weren't scheduled may report 0 GPU
    if gpu_type is not None and gpu_count == 0:
        return SU_UNKNOWN_GPU, 0, "GPU"

    # pods in weird states
    if cpu_count == 0 or memory_count == 0:
        return SU_UNKNOWN, 0, "CPU"

    known_gpu_su = {
                    GPU_A100: SU_A100_GPU,
                    GPU_A10: SU_A10_GPU,
                    GPU_MOC: SU_MOC_GPU,
            }

    # GPU count for some configs is -1 for math reasons, in reality it is 0
    su_config = { SU_CPU: { "gpu": -1, "cpu": 1, "ram": 4 },
                  SU_A100_GPU: { "gpu": 1, "cpu": 24, "ram": 96 },
                  SU_A10_GPU: { "gpu": 1, "cpu": 8, "ram": 64 },
                  SU_UNKNOWN_GPU: { "gpu": 1, "cpu": 8, "ram": 64 },
                  SU_UNKNOWN: { "gpu": -1, "cpu": 1, "ram": 1 },
                  SU_MOC_GPU: { "gpu": 1, "cpu": 24, "ram": 128 },
            }

    if gpu_type is None and gpu_count == 0:
        su_type = SU_CPU
    else:
        su_type = known_gpu_su.get(gpu_type, SU_UNKNOWN_GPU)

    # because openshift offers fractional CPUs, so we round it up.
    cpu_count = math.ceil(cpu_count)

    cpu_multiplier = cpu_count/su_config[su_type]["cpu"]
    gpu_multiplier = gpu_count/su_config[su_type]["gpu"]
    memory_multiplier = math.ceil(memory_count/su_config[su_type]["ram"])

    su_count = math.ceil(max(cpu_multiplier, gpu_multiplier, memory_multiplier))

    if cpu_multiplier >= gpu_multiplier and cpu_multiplier >= memory_multiplier:
        determining_resource = "CPU"
    elif gpu_multiplier >= cpu_multiplier and gpu_multiplier >= memory_multiplier:
        determining_resource = "GPU"
    else:
        determining_resource = "RAM"

    return su_type, su_count, determining_resource


def merge_metrics(metric_name, metric_list, output_dict):
    for metric in metric_list:
        pod = metric['metric']['pod']
        if pod not in output_dict:
            output_dict[pod] = {'namespace': metric['metric']['namespace'],
                                'metrics': {}}

        gpu_type = metric['metric']['resource']
        if gpu_type not in ['cpu', 'memory']:
            output_dict[pod]['gpu_type'] = gpu_type
        else:
            output_dict[pod]['gpu_type'] = NO_GPU

        for value in metric['values']:
            epoch_time = value[0]
            if epoch_time not in output_dict[pod]['metrics']:
                output_dict[pod]['metrics'][epoch_time] = {}
            output_dict[pod]['metrics'][epoch_time][metric_name] = value[1]
    return output_dict

def condense_metrics(input_metrics_dict, metrics_to_check):
    condensed_dict = {}
    for pod, pod_dict in input_metrics_dict.items():
        metrics_dict = pod_dict['metrics']
        new_metrics_dict = {}
        epoch_times_list = sorted(metrics_dict.keys())

        start_epoch_time = epoch_times_list[0]
        start_metric_dict = metrics_dict[start_epoch_time].copy()
        for epoch_time in epoch_times_list:
            same_metrics = True
            for metric in metrics_to_check:
                if metrics_dict[start_epoch_time].get(metric, 0) != metrics_dict[epoch_time].get(metric, 0):
                    same_metrics = False
            
            if not same_metrics:
                duration = epoch_time - start_epoch_time - 1
                start_metric_dict['duration'] = duration
                new_metrics_dict[start_epoch_time] = start_metric_dict
                start_epoch_time = epoch_time
                start_metric_dict = metrics_dict[start_epoch_time].copy()
        duration = epoch_time - start_epoch_time + 59
        start_metric_dict['duration'] = duration
        new_metrics_dict[start_epoch_time] = start_metric_dict

        new_pod_dict = pod_dict.copy()
        new_pod_dict['metrics'] = new_metrics_dict
        condensed_dict[pod] = new_pod_dict

    return condensed_dict

def write_metrics_by_namespace_differently(condensed_metrics_dict, file_name, report_start_date, report_end_date):
    metrics_by_namespace = {}
    namespace_annotations = get_namespace_annotations()
    print("Writing log to %s" % file_name)
    f = open(file_name, "w")
    headers = [
                "Namespace",
                "Coldfront_PI Name",
                "Start Date",
                "End Date",
                "_cpu_hours",
                "_memory_hours",
                "SU_CPU_HOURS",
                "SU_A100_GPU_HOURS",
                "SU_A10_GPU_HOURS",
                "SU_MOC_GPU_HOURS",
            ]
    f.write(DELIMITER.join(headers))
    f.write('\n')

    for pod in condensed_metrics_dict:
        pod_dict = condensed_metrics_dict[pod]
        namespace = pod_dict['namespace']
        pod_metrics_dict = pod_dict['metrics']
        namespace_annotation_dict = namespace_annotations.get(namespace, {})
        cf_pi = namespace_annotation_dict.get('cf_pi', namespace)
        cf_project_id = namespace_annotation_dict.get('cf_project_id', 1)
        gpu_type = pod_dict['gpu_type']

        if namespace not in metrics_by_namespace:
            metrics_by_namespace[namespace] = {'pi': cf_pi,
                                                '_cpu_hours': 0,
                                                '_memory_hours': 0,
                                                'SU_CPU_HOURS': 0,
                                                'SU_A100_GPU_HOURS': 0,
                                                'SU_A10_GPU_HOURS': 0,
                                                'SU_MOC_GPU_HOURS': 0,
                                                'total_cost': 0,
                                            }

        for epoch_time in pod_metrics_dict:
            pod_metric_dict = pod_metrics_dict[epoch_time]
            duration_in_hours = float(pod_metric_dict['duration']) / 3600
            cpu_request = float(pod_metric_dict.get('cpu_request', 0))
            gpu_request = float(pod_metric_dict.get('gpu_request', 0))
            memory_request = float(pod_metric_dict.get('memory_request', 0)) / 2**30


            if gpu_type == GPU_A100:
                _, su_count, _ = get_service_unit(float(cpu_request), memory_request, float(gpu_request), gpu_type)
                metrics_by_namespace[namespace]['SU_A100_GPU_HOURS'] += su_count * duration_in_hours
            elif gpu_type == GPU_A10:
                _, su_count, _ = get_service_unit(float(cpu_request), memory_request, float(gpu_request), gpu_type)
                metrics_by_namespace[namespace]['SU_A10_GPU_HOURS'] += su_count * duration_in_hours
            elif gpu_type == GPU_MOC:
                _, su_count, _ = get_service_unit(float(cpu_request), memory_request, float(gpu_request), gpu_type)
                metrics_by_namespace[namespace]['SU_MOC_GPU_HOURS'] += su_count * duration_in_hours
            else:
                metrics_by_namespace[namespace]['_cpu_hours'] += cpu_request * duration_in_hours
                metrics_by_namespace[namespace]['_memory_hours'] += memory_request * duration_in_hours

    for namespace in metrics_by_namespace:
        # this doesn't make much sense to me (⩺_⩹)

        metrics = metrics_by_namespace[namespace]
        cpu_multiplier = metrics['_cpu_hours']/1
        memory_multiplier = metrics['_memory_hours']/4

        su_count_hours = math.ceil(max(cpu_multiplier, memory_multiplier))

        metrics_by_namespace[namespace]['SU_CPU_HOURS'] += su_count_hours
        row = [ namespace,
                metrics['pi'],
                report_start_date,
                report_end_date,
                str(metrics['_cpu_hours']),
                str(metrics['_memory_hours']),
                str(metrics['SU_CPU_HOURS']),
                str(metrics['SU_A100_GPU_HOURS']),
                str(metrics['SU_A10_GPU_HOURS']),
                str(metrics['SU_MOC_GPU_HOURS']),
            ]
        f.write(DELIMITER.join(row))
        f.write('\n')

    f.close()


def write_metrics_by_namespace(condensed_metrics_dict, file_name, report_start_date, report_end_date):
    count = 0
    metrics_by_namespace = {}
    namespace_annotations = get_namespace_annotations()
    print("Writing log to %s" % file_name)
    f = open(file_name, "w")
    headers = [
                "Namespace",
                "Group/Coldfront_PI Name",
                "Start Date",
                "End Date",
                "Total cost",
            ]
    f.write(DELIMITER.join(headers))
    f.write('\n')
    for pod in condensed_metrics_dict:
        pod_dict = condensed_metrics_dict[pod]
        namespace = pod_dict['namespace']
        pod_metrics_dict = pod_dict['metrics']
        namespace_annotation_dict = namespace_annotations.get(namespace, {})
        cf_pi = namespace_annotation_dict.get('cf_pi', namespace)
        cf_project_id = namespace_annotation_dict.get('cf_project_id', 1)
        gpu_type = pod_dict['gpu_type']

        if namespace not in metrics_by_namespace:
            metrics_by_namespace[namespace] = {'pi': cf_pi,
                                                'total_cost': 0,
                                            }
        for epoch_time in pod_metrics_dict:
            pod_metric_dict = pod_metrics_dict[epoch_time]

            duration_in_hours = float(pod_metric_dict['duration']) / 3600
            cpu_request = float(pod_metric_dict.get('cpu_request', 0))
            gpu_request = float(pod_metric_dict.get('gpu_request', 0))
            memory_request = float(pod_metric_dict.get('memory_request', 0)) / 2**30

            su_type, su_count, _ = get_service_unit(float(cpu_request), memory_request, float(gpu_request), gpu_type)

            pod_cost = SU_COST[su_type] * su_count * duration_in_hours

            metrics_by_namespace[namespace]['total_cost'] += round(pod_cost, 4)

    for namespace in metrics_by_namespace:
        metrics = metrics_by_namespace[namespace]
        row = [ namespace,
                metrics['pi'],
                report_start_date,
                report_end_date,
                str(metrics['total_cost']),
            ]
        f.write(DELIMITER.join(row))
        f.write('\n')
    f.close()

def write_metrics_by_pod(metrics_dict, file_name, openshift_cluster_name):
    count = 0
    namespace_annotations = get_namespace_annotations()
    print("Writing log to %s" % file_name)
    f = open(file_name, "w")
    headers = [
                "Job ID",
                "Cluster Name",
                "Account Name",
                "Group/Coldfront_PI Name",
                "Group ID Number",
                "Start Time",
                "End Time",
                "Duration (Hours)",
                "Pod Name",
                "CPU Request",
                "GPU Request",
                "GPU Type",
                "Memory Request (GiB)",
                "Determining Resource",
                "SU Type",
                "Multiplier",
                "Charge Hours"
            ]
    f.write(DELIMITER.join(headers))
    f.write('\n')
    for pod in metrics_dict:
        pod_dict = metrics_dict[pod]
        namespace = pod_dict['namespace']
        pod_metrics_dict = pod_dict['metrics']
        gpu_type = pod_dict['gpu_type']
        namespace_annotation_dict = namespace_annotations.get(namespace, {})
        cf_pi = namespace_annotation_dict.get('cf_pi', namespace)
        cf_project_id = namespace_annotation_dict.get('cf_project_id', 1)

        for epoch_time in pod_metrics_dict:
            pod_metric_dict = pod_metrics_dict[epoch_time]
            job_id = count
            pod_name = pod
            cluster_name = openshift_cluster_name
            account_name = namespace
            group_name = cf_pi
            gid_number = cf_project_id
            start_time = datetime.datetime.fromtimestamp(float(epoch_time)).strftime("%Y-%m-%dT%H:%M:%S")
            end_time = datetime.datetime.fromtimestamp(float(epoch_time + pod_metric_dict['duration'])).strftime("%Y-%m-%dT%H:%M:%S")
            duration = round(float(pod_metric_dict['duration']) / 3600, 4)
            cpu_request = pod_metric_dict.get('cpu_request', 0)
            gpu_request = pod_metric_dict.get('gpu_request', 0)
            memory_request = round(float(pod_metric_dict.get('memory_request', 0)) / 2**30, 4)
            su_type, su_count, determining_resource = get_service_unit(float(cpu_request), memory_request, float(gpu_request), gpu_type)
            pod_cost = SU_COST[su_type] * su_count * duration

            info_list = [
                str(job_id),
                cluster_name,
                account_name,
                group_name,
                str(gid_number),
                start_time,
                end_time,
                str(duration),
                pod_name,
                str(cpu_request),
                str(gpu_request),
                gpu_type,
                str(memory_request),
                determining_resource,
                su_type,
                str(su_count),
                str(duration*su_count)
                ]

            f.write(DELIMITER.join(info_list))
            f.write('\n')
            count = count + 1
    f.close()
