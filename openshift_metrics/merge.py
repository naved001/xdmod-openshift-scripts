"""
Merges metrics from files and produces reports by pod and by namespace
"""

import argparse
import datetime
import json

import utils


def compare_dates(date_str1, date_str2):
    """Returns true is date1 is earlier than date2"""
    date1 = datetime.datetime.strptime(date_str1, "%Y-%m-%d")
    date2 = datetime.datetime.strptime(date_str2, "%Y-%m-%d")
    return date1 < date2


def main():
    """Reads the metrics from files and generates the reports"""
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    files = args.files
    merged_dictionary = {}
    output_file = f"{datetime.datetime.today().strftime('%Y-%m-%d')}.log"

    report_start_date = None
    report_end_date = None

    for file in files:
        with open(file, "r") as jsonfile:
            metrics_from_file = json.load(jsonfile)
            cpu_request_metrics = metrics_from_file["cpu_metrics"]
            memory_request_metrics = metrics_from_file["memory_metrics"]
            gpu_request_metrics = metrics_from_file.get("gpu_metrics", None)
            utils.merge_metrics("cpu_request", cpu_request_metrics, merged_dictionary)
            utils.merge_metrics("memory_request", memory_request_metrics, merged_dictionary)
            if gpu_request_metrics is not None:
                utils.merge_metrics("gpu_request", gpu_request_metrics, merged_dictionary)

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
    condensed_metrics_dict = utils.condense_metrics(
        merged_dictionary, ["cpu_request", "memory_request", "gpu_request"]
    )
    utils.write_metrics_by_namespace(
        condensed_metrics_dict,
        "namespace-" + output_file,
        report_start_date,
        report_end_date,
    )
    utils.write_metrics_by_pod(condensed_metrics_dict, "pod-" + output_file)


if __name__ == "__main__":
    main()
