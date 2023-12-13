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

"""Holds bunch of utility functions"""

import os
import datetime
import time
import math
import csv
import requests

import openshift


# GPU types
GPU_A100 = "nvidia.com/gpu_A100"
GPU_A2 = "nvidia.com/gpu_A2"
GPU_V100 = "nvidia.com/gpu_V100"
GPU_GENERIC = "nvidia.com/gpu"
NO_GPU = "No GPU"

# SU Types
SU_CPU = "OpenShift CPU"
SU_A100_GPU = "OpenShift GPUA100"
SU_A2_GPU = "OpenShift GPUA2"
SU_V100_GPU = "OpenShift GPUV100"
SU_UNKNOWN_GPU = "OpenShift Unknown GPU"
SU_UNKNOWN = "Openshift Unknown"

RATE = {
    SU_CPU: 0.013,
    SU_A100_GPU: 1.803,
    SU_A2_GPU: 0.466,
    SU_V100_GPU: 0.902,
    SU_UNKNOWN_GPU: 0,
}

STEP_MIN = 15


class EmptyResultError(Exception):
    """Raise when no results are retrieved for a query"""


def query_metric(openshift_url, token, metric, report_start_date, report_end_date):
    """Queries metric from prometheus/thanos for the provided openshift_url"""
    data = None
    headers = {"Authorization": f"Bearer {token}"}
    day_url_vars = f"start={report_start_date}T00:00:00Z&end={report_end_date}T23:59:59Z"
    print(f"Retrieving metric: {metric}")
    for _ in range(3):
        url = f"{openshift_url}/api/v1/query_range?query={metric}&{day_url_vars}&step={STEP_MIN}m"
        response = requests.get(url, headers=headers, verify=True)
        if response.status_code != 200:
            print(f"{response.status_code} Response: {response.reason}")
        else:
            data = response.json()["data"]["result"]
            if data:
                break
            print("Empty result set")
        time.sleep(3)
    if not data:
        raise EmptyResultError(f"Error retrieving metric: {metric}")
    return data


def get_namespace_annotations():
    """
    Returns namespace annotations
    Used for finding coldfront pi name and id
    """
    token = os.environ.get("OPENSHIFT_TOKEN")

    if token is not None:
        openshift.set_default_token(token)

    namespaces_dict = {}
    namespaces = openshift.selector("namespaces").objects()
    for namespace in namespaces:
        namespace_dict = namespace.as_dict()["metadata"]
        namespaces_dict[namespace_dict["name"]] = namespace_dict["annotations"]
    return namespaces_dict


def get_service_unit(cpu_count, memory_count, gpu_count, gpu_type):
    """
    Returns the type of service unit, the count, and the determining resource
    """
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
        GPU_A2: SU_A2_GPU,
        GPU_V100: SU_V100_GPU,
        GPU_GENERIC: SU_UNKNOWN_GPU,
    }

    # GPU count for some configs is -1 for math reasons, in reality it is 0
    su_config = {
        SU_CPU: {"gpu": -1, "cpu": 1, "ram": 4},
        SU_A100_GPU: {"gpu": 1, "cpu": 24, "ram": 96},
        SU_V100_GPU: {"gpu": 1, "cpu": 24, "ram": 96},
        SU_A2_GPU: {"gpu": 1, "cpu": 8, "ram": 64},
        SU_UNKNOWN_GPU: {"gpu": 1, "cpu": 8, "ram": 64},
        SU_UNKNOWN: {"gpu": -1, "cpu": 1, "ram": 1},
    }

    if gpu_type is None and gpu_count == 0:
        su_type = SU_CPU
    else:
        su_type = known_gpu_su.get(gpu_type, SU_UNKNOWN_GPU)

    cpu_multiplier = cpu_count / su_config[su_type]["cpu"]
    gpu_multiplier = gpu_count / su_config[su_type]["gpu"]
    memory_multiplier = memory_count / su_config[su_type]["ram"]

    su_count = max(cpu_multiplier, gpu_multiplier, memory_multiplier)

    # no fractional SUs for GPU SUs
    if su_type != SU_CPU:
        su_count = math.ceil(su_count)

    if gpu_multiplier >= cpu_multiplier and gpu_multiplier >= memory_multiplier:
        determining_resource = "GPU"
    elif cpu_multiplier >= gpu_multiplier and cpu_multiplier >= memory_multiplier:
        determining_resource = "CPU"
    else:
        determining_resource = "RAM"

    return su_type, su_count, determining_resource


def merge_metrics(metric_name, metric_list, output_dict):
    """Merge metrics by pod"""
    for metric in metric_list:
        pod = metric["metric"]["pod"]
        if pod not in output_dict:
            output_dict[pod] = {"namespace": metric["metric"]["namespace"], "metrics": {}}

        gpu_type = metric["metric"].get("resource", NO_GPU)
        if gpu_type not in ["cpu", "memory"]:
            output_dict[pod]["gpu_type"] = gpu_type
        else:
            output_dict[pod]["gpu_type"] = NO_GPU

        for value in metric["values"]:
            epoch_time = value[0]
            if epoch_time not in output_dict[pod]["metrics"]:
                output_dict[pod]["metrics"][epoch_time] = {}
            output_dict[pod]["metrics"][epoch_time][metric_name] = value[1]
    return output_dict


def condense_metrics(input_metrics_dict, metrics_to_check):
    """
    Checks if the value of metrics is the same, and removes redundant
    metrics while updating the duration
    """
    condensed_dict = {}
    for pod, pod_dict in input_metrics_dict.items():
        metrics_dict = pod_dict["metrics"]
        new_metrics_dict = {}
        epoch_times_list = sorted(metrics_dict.keys())

        start_epoch_time = epoch_times_list[0]

        # calculate the interval if we have more than 1 measurement, otherwise
        # use the STEP_MIN from the query as best guess
        if len(epoch_times_list) > 1:
            interval = epoch_times_list[1] - epoch_times_list[0]
        else:
            interval = STEP_MIN * 60

        start_metric_dict = metrics_dict[start_epoch_time].copy()
        for epoch_time in epoch_times_list:
            same_metrics = True
            for metric in metrics_to_check:
                if metrics_dict[start_epoch_time].get(metric, 0) != metrics_dict[epoch_time].get(metric, 0):  # fmt: skip
                    same_metrics = False

            if not same_metrics:
                duration = epoch_time - start_epoch_time
                start_metric_dict["duration"] = duration
                new_metrics_dict[start_epoch_time] = start_metric_dict
                start_epoch_time = epoch_time
                start_metric_dict = metrics_dict[start_epoch_time].copy()
        duration = epoch_time - start_epoch_time + interval
        start_metric_dict["duration"] = duration
        new_metrics_dict[start_epoch_time] = start_metric_dict

        new_pod_dict = pod_dict.copy()
        new_pod_dict["metrics"] = new_metrics_dict
        condensed_dict[pod] = new_pod_dict

    return condensed_dict


def csv_writer(rows, file_name):
    """Writes rows as csv to file_name"""
    print(f"Writing csv to {file_name}")
    with open(file_name, "w") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(rows)


def write_metrics_by_namespace(condensed_metrics_dict, file_name, report_month):
    """
    Process metrics dictionary to aggregate usage by namespace and then write that to a file
    """
    metrics_by_namespace = {}
    rows = []
    namespace_annotations = get_namespace_annotations()
    headers = [
        "Invoice Month",
        "Project - Allocation",
        "Project - Allocation ID",
        "Manager (PI)",
        "Invoice Email",
        "Invoice Address",
        "Institution",
        "Institution - Specific Code",
        "SU Hours (GBhr or SUhr)",
        "SU Type",
        "Rate",
        "Cost",
    ]

    rows.append(headers)

    for pod, pod_dict in condensed_metrics_dict.items():
        namespace = pod_dict["namespace"]
        pod_metrics_dict = pod_dict["metrics"]
        namespace_annotation_dict = namespace_annotations.get(namespace, {})
        cf_pi = namespace_annotation_dict.get("cf_pi", namespace)
        gpu_type = pod_dict["gpu_type"]

        if namespace not in metrics_by_namespace:
            metrics_by_namespace[namespace] = {
                "pi": cf_pi,
                "_cpu_hours": 0,
                "_memory_hours": 0,
                "SU_CPU_HOURS": 0,
                "SU_A100_GPU_HOURS": 0,
                "SU_A2_GPU_HOURS": 0,
                "SU_V100_GPU_HOURS": 0,
                "SU_UNKNOWN_GPU_HOURS": 0,
                "total_cost": 0,
            }

        for epoch_time, pod_metric_dict in pod_metrics_dict.items():
            duration_in_hours = float(pod_metric_dict["duration"]) / 3600
            cpu_request = float(pod_metric_dict.get("cpu_request", 0))
            gpu_request = float(pod_metric_dict.get("gpu_request", 0))
            memory_request = float(pod_metric_dict.get("memory_request", 0)) / 2**30

            _, su_count, _ = get_service_unit(cpu_request, memory_request, gpu_request, gpu_type)

            if gpu_type == GPU_A100:
                metrics_by_namespace[namespace]["SU_A100_GPU_HOURS"] += su_count * duration_in_hours
            elif gpu_type == GPU_A2:
                metrics_by_namespace[namespace]["SU_A2_GPU_HOURS"] += su_count * duration_in_hours
            elif gpu_type == GPU_GENERIC:
                metrics_by_namespace[namespace]["SU_UNKNOWN_GPU_HOURS"] += su_count * duration_in_hours
            else:
                metrics_by_namespace[namespace]["SU_CPU_HOURS"] += su_count * duration_in_hours

    for namespace, metrics in metrics_by_namespace.items():

        if metrics["SU_CPU_HOURS"] != 0:
            row = [
                report_month,
                namespace,
                namespace,
                metrics["pi"],
                "", #Invoice Email
                "", #Invoice Address
                "", #Institution
                "", #Institution - Specific Code
                str(math.ceil(metrics["SU_CPU_HOURS"])),
                SU_CPU,
                str(RATE.get(SU_CPU)),
                str(RATE.get(SU_CPU) * metrics["SU_CPU_HOURS"])
            ]
            rows.append(row)

        if metrics["SU_A100_GPU_HOURS"] != 0:
            row = [
                report_month,
                namespace,
                namespace,
                metrics["pi"],
                "", #Invoice Email
                "", #Invoice Address
                "", #Institution
                "", #Institution - Specific Code
                str(math.ceil(metrics["SU_A100_GPU_HOURS"])),
                SU_A100_GPU,
                str(RATE.get(SU_A100_GPU)),
                str(RATE.get(SU_A100_GPU) * math.ceil(metrics["SU_A100_GPU_HOURS"]))
            ]
            rows.append(row)

        if metrics["SU_A2_GPU_HOURS"] != 0:
            row = [
                report_month,
                namespace,
                namespace,
                metrics["pi"],
                "", #Invoice Email
                "", #Invoice Address
                "", #Institution
                "", #Institution - Specific Code
                str(math.ceil(metrics["SU_A2_GPU_HOURS"])),
                SU_A2_GPU,
                str(RATE.get(SU_A2_GPU)),
                str(RATE.get(SU_A2_GPU) * math.ceil(metrics["SU_A2_GPU_HOURS"]))
            ]
            rows.append(row)

        if metrics["SU_V100_GPU_HOURS"] != 0:
            row = [
                report_month,
                namespace,
                namespace,
                metrics["pi"],
                "", #Invoice Email
                "", #Invoice Address
                "", #Institution
                "", #Institution - Specific Code
                str(match.ceil(metrics["SU_V100_GPU_HOURS"])),
                SU_V100_GPU,
                str(RATE.get(SU_V100_GPU)),
                str(RATE.get(SU_V100_GPU) * match.ceil(metrics["SU_V100_GPU_HOURS"])) #Cost
            ]
            rows.append(row)

        if metrics["SU_UNKNOWN_GPU_HOURS"] != 0:
            row = [
                report_month,
                namespace,
                namespace,
                metrics["pi"],
                "", #Invoice Email
                "", #Invoice Address
                "", #Institution
                "", #Institution - Specific Code
                str(math.ceil(metrics["SU_UNKNOWN_GPU_HOURS"])),
                SU_UNKNOWN_GPU,
                str(RATE.get(SU_UNKNOWN_GPU)),
                str(RATE.get(SU_UNKNOWN_GPU) * math.ceil(metrics["SU_UNKNOWN_GPU_HOURS"])) #Cost
            ]
            rows.append(row)
    csv_writer(rows, file_name)


def write_metrics_by_pod(metrics_dict, file_name):
    """
    Generates metrics report by pod

    It currently includes service units for each pod, but that doesn't make sense
    as we are calculating the CPU/Memory service units at the project level
    """
    rows = []
    namespace_annotations = get_namespace_annotations()
    headers = [
        "Namespace",
        "Coldfront_PI Name",
        "Coldfront Project ID ",
        "Pod Start Time",
        "Pod End Time",
        "Duration (Hours)",
        "Pod Name",
        "CPU Request",
        "GPU Request",
        "GPU Type",
        "Memory Request (GiB)",
        "Determining Resource",
        "SU Type",
        "SU Count",
    ]
    rows.append(headers)

    for pod, pod_dict in metrics_dict.items():
        namespace = pod_dict["namespace"]
        pod_metrics_dict = pod_dict["metrics"]
        gpu_type = pod_dict["gpu_type"]
        namespace_annotation_dict = namespace_annotations.get(namespace, {})
        cf_pi = namespace_annotation_dict.get("cf_pi", namespace)
        cf_project_id = namespace_annotation_dict.get("cf_project_id", 1)

        for epoch_time, pod_metric_dict in pod_metrics_dict.items():
            start_time = datetime.datetime.utcfromtimestamp(float(epoch_time)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            end_time = datetime.datetime.utcfromtimestamp(
                float(epoch_time + pod_metric_dict["duration"])
            ).strftime("%Y-%m-%dT%H:%M:%S")
            duration = round(float(pod_metric_dict["duration"]) / 3600, 4)
            cpu_request = pod_metric_dict.get("cpu_request", 0)
            gpu_request = pod_metric_dict.get("gpu_request", 0)
            memory_request = round(float(pod_metric_dict.get("memory_request", 0)) / 2**30, 4)
            su_type, su_count, determining_resource = get_service_unit(
                float(cpu_request), memory_request, float(gpu_request), gpu_type
            )

            info_list = [
                namespace,
                cf_pi,
                cf_project_id,
                start_time,
                end_time,
                duration,
                pod,
                cpu_request,
                gpu_request,
                gpu_type,
                memory_request,
                determining_resource,
                su_type,
                su_count,
            ]

            rows.append(info_list)
    csv_writer(rows, file_name)
