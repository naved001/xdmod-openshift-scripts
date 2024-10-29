"""
Merges metrics from files and produces reports by pod and by namespace
"""

import argparse
from datetime import datetime, UTC
import json
from typing import Tuple
import os
import logging

from openshift_metrics import utils
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
        "--output-file",
        help = "Name of the invoice file. Defaults to NERC OpenShift <report_month>.csv"
    )
    parser.add_argument(
        "--output-dir",
        default = ".",
        help = "Directory where the report files are generated (defaults to current directory)"
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

    args = parser.parse_args()
    files = args.files

    ignore_hours = args.ignore_hours

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

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

    print(report_start_date)
    print(report_end_date)
    report_start_date = datetime.strptime(report_start_date, "%Y-%m-%d")
    report_end_date = datetime.strptime(report_end_date, "%Y-%m-%d")

    report_month = datetime.strftime(report_start_date, "%Y-%m")

    if args.output_file:
        invoice_file = args.output_file
    else:
        invoice_file = f"NERC OpenShift {report_month}.csv"

    pod_report_file = "Pod-" + invoice_file
    invoice_path = os.path.join(args.output_dir, invoice_file)
    pod_report_path = os.path.join(args.output_dir, pod_report_file)
    logger.info("Invoice Path is: %s", invoice_path)
    logger.info("Pod report path is: %s", pod_report_path)

    if report_start_date.month != report_end_date.month:
        print("Warning: The report spans multiple months")
        report_month += " to " + datetime.strftime(report_end_date, "%Y-%m")

    condensed_metrics_dict = processor.condense_metrics(
        ["cpu_request", "memory_request", "gpu_request", "gpu_type"]
    )
    utils.write_metrics_by_namespace(
        condensed_metrics_dict,
        invoice_path,
        report_month,
        ignore_hours,
    )
    utils.write_metrics_by_pod(condensed_metrics_dict, pod_report_path, ignore_hours)

    if args.upload_to_s3:
        primary_location = (
            f"Invoices/{report_month}/"
            f"Service Invoices/{invoice_file}"
        )
        utils.upload_to_s3(invoice_path, "nerc-invoicing", primary_location)

        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        secondary_location = (
            f"Invoices/{report_month}/"
            f"Archive/NERC OpenShift {report_month} {timestamp}.csv"
        )
        utils.upload_to_s3(invoice_path, "nerc-invoicing", secondary_location)
        pod_report = (
            f"Invoices/{report_month}/"
            f"Archive/Pod-NERC OpenShift {report_month} {timestamp}.csv"
        )
        utils.upload_to_s3(pod_report_path, "nerc-invoicing", pod_report)

if __name__ == "__main__":
    main()
