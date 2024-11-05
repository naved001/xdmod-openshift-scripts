FROM python:3.11.8-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY openshift_metrics/ /app/openshift_metrics
COPY bin/collect_metrics.sh /app/collect_metrics.sh
COPY bin/produce_report.sh /app/produce_report.sh

CMD ["./collect_metrics.sh"]
