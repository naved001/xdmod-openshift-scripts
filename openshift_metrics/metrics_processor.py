from typing import List, Dict

GPU_UNKNOWN_TYPE = "GPU_UNKNOWN_TYPE"


class MetricsProcessor:
    """Provides methods for merging metrics and processing it for billing purposes"""

    def __init__(self, interval_minutes: int = 15, merged_data: dict = None):
        self.interval_minutes = interval_minutes
        self.merged_data = merged_data if merged_data is not None else {}

    def merge_metrics(self, metric_name, metric_list):
        """Merge metrics (cpu, memory, gpu) by pod"""

        for metric in metric_list:
            pod = metric["metric"]["pod"]
            namespace = metric["metric"]["namespace"]
            node = metric["metric"].get("node")

            gpu_type = None
            gpu_resource = None
            node_model = None

            if namespace not in self.merged_data:
                self.merged_data[namespace] = {}
            if pod not in self.merged_data[namespace]:
                self.merged_data[namespace][pod] = {"metrics": {}}

            if metric_name == "gpu_request":
                gpu_type = metric["metric"].get(
                    "label_nvidia_com_gpu_product", GPU_UNKNOWN_TYPE
                )
                gpu_resource = metric["metric"].get("resource")
                node_model = metric["metric"].get("label_nvidia_com_gpu_machine")

            for value in metric["values"]:
                epoch_time = value[0]

                if epoch_time not in self.merged_data[namespace][pod]["metrics"]:
                    self.merged_data[namespace][pod]["metrics"][epoch_time] = {}

                self.merged_data[namespace][pod]["metrics"][epoch_time][
                    metric_name
                ] = value[1]
                if gpu_type:
                    self.merged_data[namespace][pod]["metrics"][epoch_time][
                        "gpu_type"
                    ] = gpu_type
                if gpu_resource:
                    self.merged_data[namespace][pod]["metrics"][epoch_time][
                        "gpu_resource"
                    ] = gpu_resource
                if node_model:
                    self.merged_data[namespace][pod]["metrics"][epoch_time][
                        "node_model"
                    ] = node_model
                if node:
                    self.merged_data[namespace][pod]["metrics"][epoch_time][
                        "node"
                    ] = node

    def condense_metrics(self, metrics_to_check: List[str]) -> Dict:
        """
        Checks if the value of metrics is the same, and removes redundant
        metrics while updating the duration. If there's a gap in the reported
        metrics then don't count that as part of duration.
        """
        interval = self.interval_minutes * 60
        condensed_dict = {}

        for namespace, pods in self.merged_data.items():

            if namespace not in condensed_dict:
                condensed_dict[namespace] = {}

            for pod, pod_dict in pods.items():

                metrics_dict = pod_dict["metrics"]
                new_metrics_dict = {}
                epoch_times_list = sorted(metrics_dict.keys())

                start_epoch_time = epoch_times_list[0]

                start_metric_dict = metrics_dict[start_epoch_time].copy()

                for i in range(len(epoch_times_list)):
                    epoch_time = epoch_times_list[i]
                    same_metrics = True
                    continuous_metrics = True
                    for metric in metrics_to_check:
                        # If either cpu, memory or gpu request is diferent.
                        if metrics_dict[start_epoch_time].get(metric, 0) != metrics_dict[epoch_time].get(metric, 0):  # fmt: skip
                            same_metrics = False

                    if i != 0 and epoch_time - epoch_times_list[i - 1] > interval:
                        # i.e. if the difference between 2 consecutive timestamps
                        # is more than the expected frequency then the pod was stopped
                        continuous_metrics = False

                    if not same_metrics or not continuous_metrics:
                        duration = epoch_times_list[i - 1] - start_epoch_time + interval
                        start_metric_dict["duration"] = duration
                        new_metrics_dict[start_epoch_time] = start_metric_dict
                        start_epoch_time = epoch_time
                        start_metric_dict = metrics_dict[start_epoch_time].copy()

                duration = epoch_time - start_epoch_time + interval
                start_metric_dict["duration"] = duration
                new_metrics_dict[start_epoch_time] = start_metric_dict

                new_pod_dict = pod_dict.copy()
                new_pod_dict["metrics"] = new_metrics_dict
                condensed_dict[namespace][pod] = new_pod_dict

        return condensed_dict
