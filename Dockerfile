FROM python:3.11.8-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY openshift_metrics/ /app/openshift_metrics

CMD ["python", "openshift_metrics/openshift_prometheus_metrics.py", "--upload-to-s3"]
