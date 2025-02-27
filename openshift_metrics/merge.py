"""
Merges metrics from files and produces reports by pod and by namespace
"""

import logging
import os
import argparse
from datetime import datetime, UTC
import json
from typing import Tuple
from decimal import Decimal
import nerc_rates

from openshift_metrics import utils, invoice
from openshift_metrics.metrics_processor import MetricsProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def compare_dates(date_str1, date_str2):
    """Returns true is date1 is earlier than date2"""
    date1 = datetime.strptime(date_str1, "%Y-%m-%d")
    date2 = datetime.strptime(date_str2, "%Y-%m-%d")
    return date1 < date2


def parse_timestamp_range(timestamp_range: str) -> Tuple[datetime, datetime]:
    try:
        start_str, end_str = timestamp_range.split(",")
        start_dt = datetime.fromisoformat(start_str).replace(tzinfo=UTC)
        end_dt = datetime.fromisoformat(end_str).replace(tzinfo=UTC)

        if start_dt > end_dt:
            raise argparse.ArgumentTypeError("Ignore start time is after ignore end time")
        return start_dt, end_dt
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Timestamp range must be in the format 'YYYY-MM-DDTHH:MM:SS,YYYY-MM-DDTHH:MM:SS'"
        )

def main():
    """Reads the metrics from files and generates the reports"""
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+")
    parser.add_argument(
        "--invoice-file",
        help = "Name of the invoice file. Defaults to NERC OpenShift <report_month>.csv"
    )
    parser.add_argument(
        "--pod-report-file",
        help = "Name of the pod report file. Defaults to Pod NERC OpenShift <report_month>.csv"
    )
    parser.add_argument(
        "--upload-to-s3",
        action="store_true"
    )
    parser.add_argument(
        "--ignore-hours",
        type=parse_timestamp_range,
        nargs="*",
        help="List of timestamp ranges in UTC to ignore in the format 'YYYY-MM-DDTHH:MM:SS,YYYY-MM-DDTHH:MM:SS'"
    )
    parser.add_argument(
        "--use-nerc-rates",
        action="store_true",
        help="Use rates from the nerc-rates repo",
    )
    parser.add_argument("--rate-cpu-su", type=Decimal)
    parser.add_argument("--rate-gpu-v100-su", type=Decimal)
    parser.add_argument("--rate-gpu-a100sxm4-su", type=Decimal)
    parser.add_argument("--rate-gpu-a100-su", type=Decimal)

    args = parser.parse_args()
    files = args.files
    ignore_hours = args.ignore_hours

    report_start_date = None
    report_end_date = None

    processor = MetricsProcessor()

    for file in files:
        with open(file, "r") as jsonfile:
            metrics_from_file = json.load(jsonfile)
            cpu_request_metrics = metrics_from_file["cpu_metrics"]
            memory_request_metrics = metrics_from_file["memory_metrics"]
            gpu_request_metrics = metrics_from_file.get("gpu_metrics", None)
            processor.merge_metrics("cpu_request", cpu_request_metrics)
            processor.merge_metrics("memory_request", memory_request_metrics)
            if gpu_request_metrics is not None:
                processor.merge_metrics("gpu_request", gpu_request_metrics)

            if report_start_date is None:
                report_start_date = metrics_from_file["start_date"]
            elif compare_dates(metrics_from_file["start_date"], report_start_date):
                report_start_date = metrics_from_file["start_date"]

            if report_end_date is None:
                report_end_date = metrics_from_file["end_date"]
            elif compare_dates(report_end_date, metrics_from_file["end_date"]):
                report_end_date = metrics_from_file["end_date"]

    logger.info(f"Generating report from {report_start_date} to {report_end_date}")

    report_start_date = datetime.strptime(report_start_date, "%Y-%m-%d")
    report_end_date = datetime.strptime(report_end_date, "%Y-%m-%d")

    report_month = datetime.strftime(report_start_date, "%Y-%m")

    if args.use_nerc_rates:
        logger.info("Using nerc rates.")
        nerc_data = nerc_rates.load_from_url()
        rates = invoice.Rates(
            cpu=Decimal(nerc_data.get_value_at("CPU SU Rate", report_month)),
            gpu_a100=Decimal(nerc_data.get_value_at("GPUA100 SU Rate", report_month)),
            gpu_a100sxm4=Decimal(nerc_data.get_value_at("GPUA100SXM4 SU Rate", report_month)),
            gpu_v100=Decimal(nerc_data.get_value_at("GPUV100 SU Rate", report_month)),
        )
    else:
        rates = invoice.Rates(
            cpu=Decimal(args.rate_cpu_su),
            gpu_a100=Decimal(args.rate_gpu_a100_su),
            gpu_a100sxm4=Decimal(args.rate_gpu_a100sxm4_su),
            gpu_v100=Decimal(args.rate_gpu_v100_su)
        )

    if args.invoice_file:
        invoice_file = args.invoice_file
    else:
        invoice_file = f"NERC OpenShift {report_month}.csv"

    if args.pod_report_file:
        pod_report_file = args.pod_report_file
    else:
        pod_report_file = f"Pod NERC OpenShift {report_month}.csv"

    if report_start_date.month != report_end_date.month:
        logger.warning("The report spans multiple months")
        report_month += " to " + datetime.strftime(report_end_date, "%Y-%m")

    condensed_metrics_dict = processor.condense_metrics(
        ["cpu_request", "memory_request", "gpu_request", "gpu_type"]
    )
    utils.write_metrics_by_namespace(
        condensed_metrics_dict=condensed_metrics_dict,
        file_name=invoice_file,
        report_month=report_month,
        rates=rates,
        ignore_hours=ignore_hours,
    )
    utils.write_metrics_by_classes(
        condensed_metrics_dict=condensed_metrics_dict,
        file_name=f"by-classes-{invoice_file}",
        report_month=report_month,
        rates=rates,
        namespaces_with_classes=["rhods-notebooks"],
        ignore_hours=ignore_hours,
    )
    utils.write_metrics_by_pod(condensed_metrics_dict, pod_report_file, ignore_hours)

    if args.upload_to_s3:
        bucket_name = os.environ.get("S3_INVOICE_BUCKET", "nerc-invoicing")
        cluster_name = os.environ.get("OPENSHIFT_CLUSTER_NAME")
        assert cluster_name, "Please set OPENSHIFT_CLUSTER_NAME to upload to S3"
        primary_location = (
            f"Invoices/{report_month}/"
            f"Service Invoices/NERC OpenShift {cluster_name} {report_month}.csv"
        )
        utils.upload_to_s3(invoice_file, bucket_name, primary_location)

        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        secondary_location = (
            f"Invoices/{report_month}/"
            f"Archive/NERC OpenShift {cluster_name} {report_month} {timestamp}.csv"
        )
        utils.upload_to_s3(invoice_file, bucket_name, secondary_location)
        pod_report_location = (
            f"Invoices/{report_month}/"
            f"Archive/Pod-NERC OpenShift {cluster_name} {report_month} {timestamp}.csv"
        )
        utils.upload_to_s3(pod_report_file, bucket_name, pod_report_location)

if __name__ == "__main__":
    main()
