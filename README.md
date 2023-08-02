# xdmod-openshift-scripts

xdmod-openshift-scripts contains a couple of scripts to pull metrics from Prometheus or Thanos
and then merge the metrics to produce a CSV report by pods and by namespaces.

The reports do the necessary maths to calcuate Service Unit hours. For non-GPU pods we sum up the CPU
and Memory usage for each namespace and then calculate the SU hours on that. GPU pods are treated separately.


At this point this repo has nothing to do with xdmod so I will move this into its own repo soon.

## Usage

In order to run the scripts, you must run `oc login` first.

When running the scripts, there are two methods of specifying the OpenShift Prometheus
endpoint. The first is through an environment variable:

```
    $ export OPENSHIFT_PROMETHEUS_URL=<prometheus url>
    $ python openshift_metrics/openshift_prometheus_metrics.py 
```

The second is directly on the command line:

```
    $ python openshift_metrics/openshift_prometheus_metrics.py --openshift-url <prometheus url>
```

By default the script will pull data from today and will go back to the specified report length.

You can also specify a different date:

```
    $ python openshift_metrics/openshift_prometheus_metrics.py --report-date 2022-03-14
```

## How It Works

The `openshift_prometheus_metrics.py` retrieves metrics at a pod level. It does so with the
following Prometheus query:

```
   <prometheus_url>/api/v1/query_range?query=<metric>&start=<report_date>T00:00:00Z&end=<report_date>T23:59:59Z&step=<step_min>m
```

This query generates samples for "step_min" minutes. The script will then merge consecutive samples
together if their metrics are the same.

The script queries the following metrics:

* *kube_pod_resource_request{unit="cores"} unless on(pod, namespace) kube_pod_status_unschedulable*
   * Cores requested by a pods that are scheduled to run.
* *'kube_pod_resource_request{unit="bytes"} unless on(pod, namespace) kube_pod_status_unschedulable'*
   * Memory (RAM) requested by pods that are scheduled to run.
* *'kube_pod_resource_request{resource=~".*gpu.*"} unless on(pod, namespace) kube_pod_status_unschedulable'*
   * GPU Requested by pods that are sheculed to run. The requested GPU resource must have the word "gpu" in it
   to be captured by this query. E.g. `nvidia.com/gpu`

The script also retrieves further information through annotations.
