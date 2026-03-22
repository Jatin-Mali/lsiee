"""System-level metrics collection."""

from __future__ import annotations

from typing import Any, Dict, List

import psutil


class SystemMetrics:
    """Collect system-wide resource metrics."""

    def get_cpu_usage(self) -> Dict[str, Any]:
        """Return CPU usage statistics."""
        return {
            "percent": psutil.cpu_percent(interval=0.1),
            "per_cpu": psutil.cpu_percent(interval=0.1, percpu=True),
            "count_logical": psutil.cpu_count(logical=True),
            "count_physical": psutil.cpu_count(logical=False),
        }

    def get_memory_usage(self) -> Dict[str, Any]:
        """Return memory and swap usage."""
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        return {
            "total_gb": memory.total / (1024**3),
            "available_gb": memory.available / (1024**3),
            "used_gb": memory.used / (1024**3),
            "percent": memory.percent,
            "swap_total_gb": swap.total / (1024**3),
            "swap_used_gb": swap.used / (1024**3),
            "swap_percent": swap.percent,
        }

    def get_disk_usage(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return disk usage for accessible partitions."""
        partitions: List[Dict[str, Any]] = []

        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
            except PermissionError:
                continue

            partitions.append(
                {
                    "device": partition.device,
                    "mountpoint": partition.mountpoint,
                    "fstype": partition.fstype,
                    "total_gb": usage.total / (1024**3),
                    "used_gb": usage.used / (1024**3),
                    "free_gb": usage.free / (1024**3),
                    "percent": usage.percent,
                }
            )

        return {"partitions": partitions}

    def get_network_stats(self) -> Dict[str, Any]:
        """Return network I/O counters."""
        network = psutil.net_io_counters()
        return {
            "bytes_sent": network.bytes_sent,
            "bytes_recv": network.bytes_recv,
            "packets_sent": network.packets_sent,
            "packets_recv": network.packets_recv,
        }

    def get_all_metrics(self) -> Dict[str, Any]:
        """Return all system metrics."""
        return {
            "cpu": self.get_cpu_usage(),
            "memory": self.get_memory_usage(),
            "disk": self.get_disk_usage(),
            "network": self.get_network_stats(),
        }
