#!/usr/bin/env sh

python -m openshift_metrics.openshift_prometheus_metrics \
    --output-file /tmp/metrics.json \
    --upload-to-s3
