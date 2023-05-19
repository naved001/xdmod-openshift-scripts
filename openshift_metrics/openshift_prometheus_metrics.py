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

import argparse
import datetime
import os
import sys

import openshift

import utils


CPU_REQUEST = 'kube_pod_resource_request{unit="cores"}'
MEMORY_REQUEST = 'kube_pod_resource_request{unit="bytes"}'
CPU_LIMIT = 'kube_pod_resource_limit{unit="cores"}'
MEMORY_LIMIT = 'kube_pod_resource_limit{unit="bytes"}'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--openshift-url", help="OpenShift Prometheus URL",
                        default=os.getenv('OPENSHIFT_PROMETHEUS_URL'))
    parser.add_argument("--openshift-cluster-name", help="OpenShift cluster name",
                        default=os.getenv('OPENSHIFT_CLUSTER_NAME'))
    parser.add_argument("--report-date", help="report date (ex: 2022-03-14)",
                        default=(datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
    parser.add_argument("--disable-ssl",
                        default=os.getenv('OPENSHIFT_DISABLE_SSL', False))
    parser.add_argument("--output-file")

    args = parser.parse_args()
    if not args.openshift_url:
        sys.exit('Must specify --openshift-url or set OPENSHIFT_PROMETHEUS_URL in your environment')
    if not args.openshift_cluster_name:
        sys.exit('Must specify --openshift-cluster-name or set OPENSHIFT_CLUSTER_NAME in your environment')
    openshift_url = args.openshift_url
    openshift_cluster_name = args.openshift_cluster_name
    report_date = args.report_date
    disable_ssl = args.disable_ssl
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = "%s.log" % report_date

    print("Generating report for %s in %s" % (report_date, output_file))

    token = openshift.get_auth_token()

    cpu_request_metrics = utils.query_metric(openshift_url, token, CPU_REQUEST, report_date, disable_ssl)
    cpu_limit_metrics = utils.query_metric(openshift_url, token, CPU_LIMIT, report_date, disable_ssl)
    memory_request_metrics = utils.query_metric(openshift_url, token, MEMORY_REQUEST, report_date, disable_ssl)
    memory_limit_metrics = utils.query_metric(openshift_url, token, MEMORY_LIMIT, report_date, disable_ssl)

    metrics_dict = {}
    utils.merge_metrics('cpu_request', cpu_request_metrics, metrics_dict)
    utils.merge_metrics('cpu_limit', cpu_limit_metrics, metrics_dict)
    utils.merge_metrics('memory_request', memory_request_metrics, metrics_dict)
    utils.merge_metrics('memory_limit', memory_limit_metrics, metrics_dict)

    condensed_metrics_dict = utils.condense_metrics(
        metrics_dict, ['cpu_request', 'cpu_limit', 'memory_request', 'memory_limit'])

    utils.write_metrics_by_namespace(condensed_metrics_dict, 'namespace-' + output_file)
    utils.write_metrics_by_pod(condensed_metrics_dict, 'pod-' + output_file, openshift_cluster_name)

if __name__ == '__main__':
    main()
