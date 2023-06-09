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

import openshift

DELIMITER = '|'

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


def get_service_unit(cpu_count, memory_count, gpu_count, gpu_type=None):
    if gpu_type == "No GPU":
        gpu_type = None
        gpu_count = 0
    flavor_dict = {
                    "cpu-su.1": {"gpu": 0, "cpu": 1, "ram": 4, "cost": 0.016 },
                    "cpu-su.2": {"gpu": 0, "cpu": 2, "ram": 8, "cost": 0.032 },
                    "cpu-su.4": {"gpu": 0, "cpu": 4, "ram": 16, "cost": 0.064 },
                    "cpu-su.8": {"gpu": 0, "cpu": 8, "ram": 32, "cost": 0.128 },
                    "cpu-su.16": {"gpu": 0, "cpu": 16, "ram": 64, "cost": 0.256 },
                    "mem-a.1": {"gpu": 0, "cpu": 1, "ram": 8, "cost": 0.032 },
                    "mem-a.2": {"gpu": 0, "cpu": 2, "ram": 16, "cost": 0.064 },
                    "mem-a.4": {"gpu": 0, "cpu": 4, "ram": 32, "cost": 0.128 },
                    "mem-a.8": {"gpu": 0, "cpu": 8, "ram": 64, "cost": 0.256 },
                    "mem-a.16": {"gpu": 0, "cpu": 16, "ram": 128, "cost": 0.512 },
                    "gpu-su-a100.1": {"gpu": 1, "cpu": 24, "ram": 96, "cost": 2.633 },
                    "gpu-su-a100.2": {"gpu": 1, "cpu": 48, "ram": 192, "cost": 5.266 },
                    "gpu-su-a100.4": {"gpu": 1, "cpu": 96, "ram": 384, "cost": 10.532 },
                    "nvidia.com/gpu": {"gpu": 1, "cpu": 24, "ram": 256, "cost": 0.512 },
                    "gpu-su-a2.1": {"gpu": 1, "cpu": 6, "ram": 32, "cost": 0.463 },
                    "gpu-su-a2.2": {"gpu": 2, "cpu": 12, "ram": 64, "cost": 0.926 },
                    "gpu-su-a2.4": {"gpu": 4, "cpu": 24, "ram": 128, "cost": 1.852 },
                    "gpu-su-a2.8": {"gpu": 8, "cpu": 48, "ram": 254, "cost": 3.704 },
                }

    if cpu_count == 0 or memory_count == 0:
        return "Unknown", 0

    best_flavor = None
    min_cost = float('inf')

    for flavor, specs in flavor_dict.items():
        if specs['cpu'] >= cpu_count and specs['ram'] >= memory_count:
            if gpu_count == 0 and specs['gpu'] == 0:  # No GPU requested, only consider flavors without GPU
                cost = specs['cost']
                if cost < min_cost:
                    best_flavor = flavor
                    min_cost = cost
            elif specs['gpu'] >= gpu_count:
                if gpu_type is None or flavor.startswith(gpu_type):
                    # this may need to be updated based on how the GPU resources are named in kubernetes
                    cost = specs['cost']
                    if cost < min_cost:
                        best_flavor = flavor
                        min_cost = cost

    return best_flavor, min_cost


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
            output_dict[pod]['gpu_type'] = 'No GPU'

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

            su_type, su_price = get_service_unit(float(cpu_request), memory_request, float(gpu_request), gpu_type)

            su_charge = round(su_price*duration_in_hours, 4)

            metrics_by_namespace[namespace]['total_cost'] += round(su_price*duration_in_hours, 4)

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
                "Duration (hours)",
                "Pod Name",
                "CPU Request",
                "GPU Request",
                "GPU Type",
                "Memory Request (GiB)",
                "SU Type",
                "SU Price",
                "SU Charge"
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
            su_type, su_price = get_service_unit(float(cpu_request), memory_request, float(gpu_request), gpu_type)
            su_charge = round(su_price*duration, 4)

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
                str(su_type),
                str(su_price),
                str(su_charge),
                ]

            f.write(DELIMITER.join(info_list))
            f.write('\n')
            count = count + 1
    f.close()
