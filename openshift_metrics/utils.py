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
import boto3

from openshift_metrics import invoice
from decimal import Decimal
import decimal
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# GPU types
GPU_A100 = "NVIDIA-A100-40GB"
GPU_A100_SXM4 = "NVIDIA-A100-SXM4-40GB"
GPU_V100 = "Tesla-V100-PCIE-32GB"
GPU_UNKNOWN_TYPE = "GPU_UNKNOWN_TYPE"

# GPU Resource - MIG Geometries
# A100 Strategies
MIG_1G_5GB = "nvidia.com/mig-1g.5gb"
MIG_2G_10GB = "nvidia.com/mig-2g.10gb"
MIG_3G_20GB = "nvidia.com/mig-3g.20gb"
WHOLE_GPU = "nvidia.com/gpu"


# SU Types
SU_CPU = "OpenShift CPU"
SU_A100_GPU = "OpenShift GPUA100"
SU_A100_SXM4_GPU = "OpenShift GPUA100SXM4"
SU_V100_GPU = "OpenShift GPUV100"
SU_UNKNOWN_GPU = "OpenShift Unknown GPU"
SU_UNKNOWN_MIG_GPU = "OpenShift Unknown MIG GPU"
SU_UNKNOWN = "Openshift Unknown"

RATE = {
    SU_CPU: Decimal("0.013"),
    SU_A100_GPU: Decimal("1.803"),
    SU_A100_SXM4_GPU: Decimal("2.078"),
    SU_V100_GPU: Decimal("1.214"),
    SU_UNKNOWN_GPU: Decimal("0"),
}

STEP_MIN = 15


class EmptyResultError(Exception):
    """Raise when no results are retrieved for a query"""


class ColdFrontClient(object):

    def __init__(self, keycloak_url, keycloak_client_id, keycloak_client_secret):
        self.session = self.get_session(keycloak_url,
                                        keycloak_client_id,
                                        keycloak_client_secret)

    @staticmethod
    def get_session(keycloak_url, keycloak_client_id, keycloak_client_secret):
        """Authenticate as a client with Keycloak to receive an access token."""
        token_url = f"{keycloak_url}/auth/realms/mss/protocol/openid-connect/token"

        r = requests.post(
            token_url,
            data={"grant_type": "client_credentials"},
            auth=requests.auth.HTTPBasicAuth(keycloak_client_id, keycloak_client_secret),
        )
        client_token = r.json()["access_token"]

        session = requests.session()
        headers = {
            "Authorization": f"Bearer {client_token}",
            "Content-Type": "application/json",
        }
        session.headers.update(headers)
        return session


def upload_to_s3(file, bucket, location):
    s3_endpoint = os.getenv("S3_OUTPUT_ENDPOINT_URL",
                            "https://s3.us-east-005.backblazeb2.com")
    s3_key_id = os.getenv("S3_OUTPUT_ACCESS_KEY_ID")
    s3_secret = os.getenv("S3_OUTPUT_SECRET_ACCESS_KEY")

    if not s3_key_id or not s3_secret:
        raise Exception("Must provide S3_OUTPUT_ACCESS_KEY_ID and"
                        " S3_OUTPUT_SECRET_ACCESS_KEY environment variables.")
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_key_id,
        aws_secret_access_key=s3_secret,
    )

    response = s3.upload_file(file, Bucket=bucket, Key=location)


def get_namespace_attributes():
    """
    Returns allocation attributes from coldfront associated
    with all projects/namespaces.

    Used for finding coldfront PI name and institution ID.
    """
    client = ColdFrontClient(
        "https://keycloak.mss.mghpcc.org",
        os.environ.get("CLIENT_ID"),
        os.environ.get("CLIENT_SECRET")
    )

    coldfront_url = os.environ.get("COLDFRONT_URL",
        "https://coldfront.mss.mghpcc.org/api/allocations?all=true")
    responses = client.session.get(coldfront_url)

    namespaces_dict = {}

    for response in responses.json():
        project_name = response["attributes"].get("Allocated Project Name")
        cf_pi = response["project"].get("pi", project_name)
        cf_project_id = response["project"].get("id", 0)
        institution_code = response["attributes"].get("Institution-Specific Code", "")
        namespaces_dict[project_name] = { "cf_pi": cf_pi, "cf_project_id": cf_project_id, "institution_code": institution_code }

    return namespaces_dict


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
    invoices = {}
    rows = []
    namespace_annotations = get_namespace_attributes()
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

    # TODO: the caller will pass in the rates as an argument
    rates = invoice.Rates(
        cpu = Decimal("0.013"),
        gpu_a100 = Decimal("1.803"),
        gpu_a100sxm4 = Decimal("2.078"),
        gpu_v100 = Decimal("1.214")
    )

    for namespace, pods in condensed_metrics_dict.items():
        namespace_annotation_dict = namespace_annotations.get(namespace, {})
        cf_pi = namespace_annotation_dict.get("cf_pi")
        cf_institution_code = namespace_annotation_dict.get("institution_code", "")

        if namespace not in invoices:
            project_invoice = invoice.ProjectInvoce(
                invoice_month=report_month,
                project=namespace,
                project_id=namespace,
                pi=cf_pi,
                invoice_email="",
                invoice_address="",
                intitution="",
                institution_specific_code=cf_institution_code,
                rates=rates
            )
            invoices[namespace] = project_invoice

        project_invoice = invoices[namespace]

        for pod, pod_dict in pods.items():
            for epoch_time, pod_metric_dict in pod_dict["metrics"].items():
                pod_obj = invoice.Pod(
                    pod_name=pod,
                    namespace=namespace,
                    start_time=epoch_time,
                    duration=pod_metric_dict["duration"],
                    cpu_request=Decimal(pod_metric_dict.get("cpu_request", 0)),
                    gpu_request=Decimal(pod_metric_dict.get("gpu_request", 0)),
                    memory_request=Decimal(pod_metric_dict.get("memory_request", 0)) / 2**30,
                    gpu_type=pod_metric_dict.get("gpu_type"),
                    gpu_resource=pod_metric_dict.get("gpu_resource"),
                    node_hostname=pod_metric_dict.get("node"),
                    node_model=pod_metric_dict.get("node_model"),
                )
                project_invoice.add_pod(pod_obj)

    for project_invoice in invoices.values():
        rows.extend(project_invoice.generate_invoice_rows(report_month))

    csv_writer(rows, file_name)


def write_metrics_by_pod(metrics_dict, file_name):
    """
    Generates metrics report by pod

    It currently includes service units for each pod, but that doesn't make sense
    as we are calculating the CPU/Memory service units at the project level
    """
    rows = []
    namespace_annotations = get_namespace_attributes()
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
        "GPU Resource",
        "Node",
        "Node Model",
        "Memory Request (GiB)",
        "Determining Resource",
        "SU Type",
        "SU Count",
    ]
    rows.append(headers)

    for namespace, pods in metrics_dict.items():
        for pod, pod_dict in pods.items():
            pod_metrics_dict = pod_dict["metrics"]
            namespace_annotation_dict = namespace_annotations.get(namespace, {})
            cf_pi = namespace_annotation_dict.get("cf_pi")
            cf_project_id = namespace_annotation_dict.get("cf_project_id")

            for epoch_time, pod_metric_dict in pod_metrics_dict.items():
                start_time = datetime.datetime.utcfromtimestamp(float(epoch_time)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                end_time = datetime.datetime.utcfromtimestamp(
                    float(epoch_time + pod_metric_dict["duration"])
                ).strftime("%Y-%m-%dT%H:%M:%S")
                duration = (Decimal(pod_metric_dict["duration"]) / 3600).quantize(Decimal(".0001"), rounding=decimal.ROUND_HALF_UP)
                cpu_request = Decimal(pod_metric_dict.get("cpu_request", 0))
                gpu_request = Decimal(pod_metric_dict.get("gpu_request", 0))
                gpu_type = pod_metric_dict.get("gpu_type")
                gpu_resource = pod_metric_dict.get("gpu_resource")
                node = pod_metric_dict.get("node", "Unknown Node")
                node_model = pod_metric_dict.get("node_model", "Unknown Model")
                memory_request = (Decimal(pod_metric_dict.get("memory_request", 0)) / 2**30).quantize(Decimal(".0001"), rounding=decimal.ROUND_HALF_UP)
                su_type, su_count, determining_resource = invoice.Pod.get_service_unit(
                    cpu_request, memory_request, gpu_request, gpu_type, gpu_resource
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
                    gpu_resource,
                    node,
                    node_model,
                    memory_request,
                    determining_resource,
                    su_type,
                    su_count,
                ]

                rows.append(info_list)

    csv_writer(rows, file_name)
