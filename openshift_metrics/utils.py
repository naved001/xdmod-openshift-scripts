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
        raise Exception('Error retrieving metric: %s' % metric)
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

def merge_metrics(metric_name, metric_list, output_dict):
    for metric in metric_list:
        pod = metric['metric']['pod']
        if pod not in output_dict:
            output_dict[pod] = {'namespace': metric['metric']['namespace'],
                                'metrics': {}}
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
                "CPU Request Hours",
                "GPU Request Hours",
                "Memory Request Hours",
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

        if namespace not in metrics_by_namespace:
            metrics_by_namespace[namespace] = {'pi': cf_pi,
                                                'cpu_request_hours': 0,
                                                'memory_request_hours': 0,
                                                'gpu_request_hours': 0,
                                            }

        for epoch_time in pod_metrics_dict:
            pod_metric_dict = pod_metrics_dict[epoch_time]

            duration_in_hours = float(pod_metric_dict['duration']) / 3600
            cpu_request = float(pod_metric_dict.get('cpu_request', 0))
            gpu_request = float(pod_metric_dict.get('gpu_request', 0))
            memory_request = float(pod_metric_dict.get('memory_request', 0)) / 2**20

            metrics_by_namespace[namespace]['cpu_request_hours'] += round(cpu_request*duration_in_hours, 4)
            metrics_by_namespace[namespace]['gpu_request_hours'] += round(gpu_request*duration_in_hours, 4)
            metrics_by_namespace[namespace]['memory_request_hours'] += round(memory_request*duration_in_hours, 4)

    for namespace in metrics_by_namespace:
        metrics = metrics_by_namespace[namespace]
        row = [ namespace,
                metrics['pi'],
                report_start_date,
                report_end_date,
                str(metrics['cpu_request_hours']),
                str(metrics['gpu_request_hours']),
                str(metrics['memory_request_hours']),
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
                "Duration (sec)",
                "CPU Request",
                "GPU Request",
                "Memory Request (MiB)",
                "Pod Name"
            ]
    f.write(DELIMITER.join(headers))
    f.write('\n')
    for pod in metrics_dict:
        pod_dict = metrics_dict[pod]
        namespace = pod_dict['namespace']
        pod_metrics_dict = pod_dict['metrics']
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
            duration = pod_metric_dict['duration']
            cpu_request = pod_metric_dict.get('cpu_request', 0)
            gpu_request = pod_metric_dict.get('gpu_request', 0)
            memory_request = float(pod_metric_dict.get('memory_request', 0)) / 2**20

            info_list = [
                str(job_id),
                cluster_name,
                account_name,
                group_name,
                str(gid_number),
                start_time,
                end_time,
                str(duration),
                str(cpu_request),
                str(gpu_request),
                str(memory_request),
                pod_name
                ]

            f.write(DELIMITER.join(info_list))
            f.write('\n')
            count = count + 1
    f.close()
