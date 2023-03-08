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


def query_metric(openshift_url, token, metric, report_date, disable_ssl=False,
                 retry=3):
    attempt = 0
    data = None
    headers = {'Authorization': "Bearer %s" % token}
    day_url_vars = "start=%sT00:00:00Z&end=%sT23:59:59Z" % (report_date, report_date)
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

def write_metrics_log(metrics_dict, file_name, openshift_cluster_name):
    count = 0
    namespace_annotations = get_namespace_annotations()
    print("Writing log to %s" % file_name)
    f = open(file_name, "w")
    for pod in metrics_dict:
        pod_dict = metrics_dict[pod]
        namespace = pod_dict['namespace']
        pod_metrics_dict = pod_dict['metrics']
        namespace_annotation_dict = namespace_annotations.get(namespace, {})
        cf_pi = namespace_annotation_dict.get('cf_pi', namespace)
        cf_project_id = namespace_annotation_dict.get('cf_project_id', namespace)

        for epoch_time in pod_metrics_dict:
            pod_metric_dict = pod_metrics_dict[epoch_time]
            job_id = count
            job_name = pod
            cluster_name = openshift_cluster_name
            partition_name = ''
            qos_name = ''
            account_name = namespace
            group_name = namespace
            gid_number = ''
            user_name = ''
            uid_number = ''
            start_time = datetime.datetime.fromtimestamp(float(epoch_time)).strftime("%Y-%m-%dT%H:%M:%S")
            end_time = datetime.datetime.fromtimestamp(float(epoch_time + pod_metric_dict['duration'])).strftime("%Y-%m-%dT%H:%M:%S")
            submission_time = start_time
            eligible_time = start_time
            duration = '0-%s' % datetime.timedelta(seconds=pod_metric_dict['duration'])
            status = 'COMPLETED'
            node_count = 1
            cpu = pod_metric_dict.get('cpu', 0)
            req_cpu = pod_metric_dict.get('allocated_cpu', 0)
            req_mem = float(pod_metric_dict.get('allocated_memory', 0)) / 1048576
            req_tres = 'cpu=%s,mem=%s' % (req_cpu, req_mem)
            alloc_tres = req_tres
            info_list = [
                str(job_id), str(job_id), cluster_name, partition_name, qos_name, account_name, group_name, '', user_name, '',
                submission_time, eligible_time, start_time, end_time, duration, '', status, str(node_count),
                str(cpu), str(req_cpu), str(req_mem), req_tres, alloc_tres,
                duration, '', job_name
            ]
            f.write('|'.join(info_list))
            f.write('\n')
            count = count + 1
    f.close()
