import math
from dataclasses import dataclass, field
from collections import namedtuple
from typing import List
from decimal import Decimal, ROUND_HALF_UP

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

ServiceUnit = namedtuple("ServiceUnit", ["su_type", "su_count", "determinig_resource"])

@dataclass
class Pod:
    """Object that represents a pod"""
    pod_name: str
    namespace: str
    start_time: int
    duration: int
    cpu_request: Decimal
    gpu_request: Decimal
    memory_request: Decimal
    gpu_type: str
    gpu_resource: str
    node_hostname: str
    node_model: str

    @staticmethod
    def get_service_unit(cpu_count, memory_count, gpu_count, gpu_type, gpu_resource) -> ServiceUnit:
        """
        Returns the type of service unit, the count, and the determining resource
        """
        su_type = SU_UNKNOWN
        su_count = 0

        # pods that requested a specific GPU but weren't scheduled may report 0 GPU
        if gpu_resource is not None and gpu_count == 0:
            return ServiceUnit(SU_UNKNOWN_GPU, 0, "GPU")

        # pods in weird states
        if cpu_count == 0 or memory_count == 0:
            return ServiceUnit(SU_UNKNOWN, 0, "CPU")

        known_gpu_su = {
            GPU_A100: SU_A100_GPU,
            GPU_A100_SXM4: SU_A100_SXM4_GPU,
            GPU_V100: SU_V100_GPU,
        }

        A100_SXM4_MIG = {
            MIG_1G_5GB: SU_UNKNOWN_MIG_GPU,
            MIG_2G_10GB: SU_UNKNOWN_MIG_GPU,
            MIG_3G_20GB: SU_UNKNOWN_MIG_GPU,
        }

        # GPU count for some configs is -1 for math reasons, in reality it is 0
        su_config = {
            SU_CPU: {"gpu": -1, "cpu": 1, "ram": 4},
            SU_A100_GPU: {"gpu": 1, "cpu": 24, "ram": 74},
            SU_A100_SXM4_GPU: {"gpu": 1, "cpu": 32, "ram": 245},
            SU_V100_GPU: {"gpu": 1, "cpu": 24, "ram": 192},
            SU_UNKNOWN_GPU: {"gpu": 1, "cpu": 8, "ram": 64},
            SU_UNKNOWN_MIG_GPU: {"gpu": 1, "cpu": 8, "ram": 64},
            SU_UNKNOWN: {"gpu": -1, "cpu": 1, "ram": 1},
        }

        if gpu_resource is None and gpu_count == 0:
            su_type = SU_CPU
        elif gpu_type is not None and gpu_resource == WHOLE_GPU:
            su_type = known_gpu_su.get(gpu_type, SU_UNKNOWN_GPU)
        elif gpu_type == GPU_A100_SXM4:  # for MIG GPU of type A100_SXM4
            su_type = A100_SXM4_MIG.get(gpu_resource, SU_UNKNOWN_MIG_GPU)
        else:
            return ServiceUnit(SU_UNKNOWN_GPU, 0, "GPU")

        cpu_multiplier = cpu_count / su_config[su_type]["cpu"]
        gpu_multiplier = gpu_count / su_config[su_type]["gpu"]
        memory_multiplier = memory_count / su_config[su_type]["ram"]

        su_count = max(cpu_multiplier, gpu_multiplier, memory_multiplier)

        # no fractional SUs for GPU SUs
        if su_type != SU_CPU:
            su_count = math.ceil(su_count)

        if gpu_multiplier >= cpu_multiplier and gpu_multiplier >= memory_multiplier:
            determining_resource = "GPU"
        elif cpu_multiplier >= gpu_multiplier and cpu_multiplier >= memory_multiplier:
            determining_resource = "CPU"
        else:
            determining_resource = "RAM"

        return ServiceUnit(su_type, su_count, determining_resource)

    def get_runtime(self) -> Decimal:
        """Return runtime eligible for billing in hours"""
        return Decimal(self.duration) / 3600


@dataclass()
class Rates:
    cpu: Decimal
    gpu_a100: Decimal
    gpu_a100sxm4: Decimal
    gpu_v100: Decimal


@dataclass
class ProjectInvoce:
    """Represents the invoicing data for a project."""

    invoice_month: str
    project: str
    project_id: str
    pi: str
    invoice_email: str
    invoice_address: str
    intitution: str
    institution_specific_code: str
    rates: Rates
    su_hours: dict = field(
        default_factory=lambda: {
            SU_CPU: 0,
            SU_A100_GPU: 0,
            SU_A100_SXM4_GPU: 0,
            SU_V100_GPU: 0,
            SU_UNKNOWN_GPU: 0,
            SU_UNKNOWN_MIG_GPU: 0,
            SU_UNKNOWN: 0,
        }
    )

    def add_pod(self, pod: Pod) -> None:
        """Aggregate a pods data"""
        su_type, su_count, _ = Pod.get_service_unit(
            cpu_count=pod.cpu_request,
            memory_count=pod.memory_request,
            gpu_count=pod.gpu_request,
            gpu_type=pod.gpu_type,
            gpu_resource=pod.gpu_resource,
        )
        duration_in_hours = pod.get_runtime()
        self.su_hours[su_type] += su_count * duration_in_hours

    def get_rate(self, su_type) -> Decimal:
        if su_type == SU_CPU:
            return self.rates.cpu
        if su_type == SU_A100_GPU:
            return self.rates.gpu_a100
        if su_type == SU_A100_SXM4_GPU:
            return self.rates.gpu_a100sxm4
        if su_type == SU_V100_GPU:
            return self.rates.gpu_v100
        return Decimal(0)

    def generate_invoice_rows(self, report_month) -> List[str]:
        rows = []
        for su_type, hours in self.su_hours.items():
            if hours > 0:
                hours = math.ceil(hours)
                rate = self.get_rate(su_type)
                cost = (rate * hours).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
                row = [
                    report_month,
                    self.project,
                    self.project_id,
                    self.pi,
                    self.invoice_email,
                    self.invoice_address,
                    self.intitution,
                    self.institution_specific_code,
                    hours,
                    su_type,
                    rate,
                    cost,
                ]
                rows.append(row)
        return rows
