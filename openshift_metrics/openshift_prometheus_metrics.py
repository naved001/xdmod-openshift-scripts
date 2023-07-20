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

"""Collect and save metrics from prometheus"""

import argparse
import datetime
import os
import sys
import json

import openshift

import utils


CPU_REQUEST = 'kube_pod_resource_request{unit="cores"} unless on(pod, namespace) kube_pod_status_unschedulable'
MEMORY_REQUEST = 'kube_pod_resource_request{unit="bytes"} unless on(pod, namespace) kube_pod_status_unschedulable'
GPU_REQUEST = 'kube_pod_resource_request{resource=~".*gpu.*"} unless on(pod, namespace) kube_pod_status_unschedulable'


def main():
    """This method kick starts the process of collecting and saving the metrics"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--openshift-url",
        help="OpenShift Prometheus URL",
        default=os.getenv("OPENSHIFT_PROMETHEUS_URL"),
    )
    parser.add_argument(
        "--report-date",
        help="report date (ex: 2022-03-14)",
        default=(datetime.datetime.today()).strftime("%Y-%m-%d"),
    )
    parser.add_argument("--report-length", help="length of report in days", default=15)
    parser.add_argument("--output-file")

    args = parser.parse_args()
    if not args.openshift_url:
        sys.exit("Must specify --openshift-url or set OPENSHIFT_PROMETHEUS_URL in your environment")
    openshift_url = args.openshift_url
    report_date = args.report_date
    report_length = int(args.report_length)
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = f"{report_date}.json"

    print(f"Generating report for {report_date} in {output_file}")

    token = openshift.get_auth_token()

    metrics_dict = {}

    report_end_date = report_date
    report_start_date = (
        datetime.datetime.strptime(report_end_date, "%Y-%m-%d")
        - datetime.timedelta(days=report_length - 1)
    ).strftime("%Y-%m-%d")

    print(report_start_date)
    print(report_end_date)
    metrics_dict["start_date"] = report_start_date
    metrics_dict["end_date"] = report_end_date

    cpu_request_metrics = utils.query_metric(
        openshift_url, token, CPU_REQUEST, report_start_date, report_end_date
    )
    memory_request_metrics = utils.query_metric(
        openshift_url, token, MEMORY_REQUEST, report_start_date, report_end_date
    )
    metrics_dict["cpu_metrics"] = cpu_request_metrics
    metrics_dict["memory_metrics"] = memory_request_metrics

    # because if nobody requests a GPU then we will get an empty set
    try:
        gpu_request_metrics = utils.query_metric(
            openshift_url, token, GPU_REQUEST, report_start_date, report_end_date
        )
        metrics_dict["gpu_metrics"] = gpu_request_metrics
    except utils.EmptyResultError:
        pass

    with open("metrics-" + output_file, "w") as file:
        json.dump(metrics_dict, file)


if __name__ == "__main__":
    main()
