#!/usr/bin/env python3
"""
Prometheus Metrics Exporter for Scheduler Research
===================================================

This script exports relevant metrics from Prometheus for scheduler comparison.
It queries Prometheus API and saves metrics to JSON/CSV for analysis.

Usage:
    python export_metrics.py --prometheus http://localhost:9090 --scheduler default --output data/
"""

import argparse
import json
import csv
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)


# Prometheus queries for scheduler research (Control Plane Load Focus)
METRICS_QUERIES = {
    # Scheduler Performance & Latency
    "scheduler_scheduling_latency_p99": 'histogram_quantile(0.99, sum(rate(scheduler_scheduling_duration_seconds_bucket[5m])) by (le))',
    "scheduler_scheduling_latency_p95": 'histogram_quantile(0.95, sum(rate(scheduler_scheduling_duration_seconds_bucket[5m])) by (le))',
    "scheduler_scheduling_latency_p50": 'histogram_quantile(0.50, sum(rate(scheduler_scheduling_duration_seconds_bucket[5m])) by (le))',
    "scheduler_pending_pods": 'scheduler_pending_pods',
    "scheduler_attempts_success": 'sum(scheduler_schedule_attempts_total{result="scheduled"})',
    "scheduler_attempts_error": 'sum(scheduler_schedule_attempts_total{result="error"})',
    "scheduler_attempts_unschedulable": 'sum(scheduler_schedule_attempts_total{result="unschedulable"})',
    
    # Kube-Scheduler Control-Plane Overhead
    "scheduler_cpu_usage_millicores": 'sum(rate(container_cpu_usage_seconds_total{container="kube-scheduler"}[5m])) * 1000',
    "scheduler_memory_mb": 'sum(container_memory_working_set_bytes{container="kube-scheduler"}) / 1024 / 1024',
    
    # API Server Control-Plane Overhead
    "apiserver_request_rate": 'sum(rate(apiserver_request_total[5m]))',
    "apiserver_request_latency_p99": 'histogram_quantile(0.99, sum(rate(apiserver_request_duration_seconds_bucket[5m])) by (le))',
    "apiserver_cpu_usage_millicores": 'sum(rate(container_cpu_usage_seconds_total{container="kube-apiserver"}[5m])) * 1000',
    "apiserver_memory_mb": 'sum(container_memory_working_set_bytes{container="kube-apiserver"}) / 1024 / 1024',
    
    # etcd Control-Plane Overhead
    "etcd_cpu_usage_millicores": 'sum(rate(container_cpu_usage_seconds_total{container="etcd"}[5m])) * 1000',
    "etcd_memory_mb": 'sum(container_memory_working_set_bytes{container="etcd"}) / 1024 / 1024',
    "etcd_request_rate": 'sum(rate(etcd_request_duration_seconds_count[5m]))',
    "etcd_db_size_mb": 'etcd_mvcc_db_total_size_in_bytes / 1024 / 1024',
    
    # Controller Manager Control-Plane Overhead
    "controller_manager_cpu_millicores": 'sum(rate(container_cpu_usage_seconds_total{container="kube-controller-manager"}[5m])) * 1000',
    "controller_manager_memory_mb": 'sum(container_memory_working_set_bytes{container="kube-controller-manager"}) / 1024 / 1024',
    
    # Total Control-Plane Aggregated Consumption
    "control_plane_total_cpu_millicores": 'sum(rate(container_cpu_usage_seconds_total{container=~"kube-scheduler|kube-apiserver|etcd|kube-controller-manager"}[5m])) * 1000',
    "control_plane_total_memory_mb": 'sum(container_memory_working_set_bytes{container=~"kube-scheduler|kube-apiserver|etcd|kube-controller-manager"}) / 1024 / 1024',
    
    # Node Cluster Utilization
    "node_cpu_utilization_percent": 'avg(100 - (avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100))',
    "node_memory_utilization_percent": 'avg((1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100)',
    
    # Pod States & Failure Rates
    "total_running_pods": 'count(kube_pod_status_phase{phase="Running"})',
    "total_pending_pods": 'count(kube_pod_status_phase{phase="Pending"})',
    "total_failed_pods": 'count(kube_pod_status_phase{phase="Failed"})',
}

# Volcano-specific metrics
VOLCANO_QUERIES = {
    "volcano_scheduler_cpu_millicores": 'sum(rate(container_cpu_usage_seconds_total{container="volcano-scheduler"}[5m])) * 1000',
    "volcano_scheduler_memory_mb": 'sum(container_memory_working_set_bytes{container="volcano-scheduler"}) / 1024 / 1024',
    "volcano_controller_cpu_millicores": 'sum(rate(container_cpu_usage_seconds_total{container="volcano-controllers"}[5m])) * 1000',
    "volcano_controller_memory_mb": 'sum(container_memory_working_set_bytes{container="volcano-controllers"}) / 1024 / 1024',
}

# NEXUS-specific metrics
NEXUS_QUERIES = {
    "nexus_scheduler_state": 'nexus_scheduler_state',
    "nexus_pending_pods": 'nexus_pending_pods',
    "nexus_pods_scheduled_total": 'nexus_pods_scheduled_total',
    "nexus_state_changes_total": 'nexus_state_changes_total',
    "nexus_scheduler_cpu_millicores": 'sum(rate(container_cpu_usage_seconds_total{container="nexus-scheduler"}[5m])) * 1000',
    "nexus_scheduler_memory_mb": 'sum(container_memory_working_set_bytes{container="nexus-scheduler"}) / 1024 / 1024',
}



class PrometheusClient:
    """Simple Prometheus HTTP API client."""
    
    def __init__(self, url: str):
        self.base_url = url.rstrip('/')
    
    def query(self, query: str) -> Optional[float]:
        """Execute an instant query and return the value."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/query",
                params={"query": query},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            if data["status"] == "success" and data["data"]["result"]:
                result = data["data"]["result"][0]
                value = float(result["value"][1])
                return value if not (value != value) else None  # NaN check
            return None
        except Exception as e:
            print(f"  Warning: Query failed - {query[:50]}... ({e})")
            return None
    
    def query_range(self, query: str, start: datetime, end: datetime, step: str = "15s") -> List[tuple]:
        """Execute a range query and return time series."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/query_range",
                params={
                    "query": query,
                    "start": start.isoformat() + "Z",
                    "end": end.isoformat() + "Z",
                    "step": step,
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            if data["status"] == "success" and data["data"]["result"]:
                values = data["data"]["result"][0].get("values", [])
                return [(float(ts), float(val)) for ts, val in values if val != "NaN"]
            return []
        except Exception as e:
            print(f"  Warning: Range query failed - {e}")
            return []


def export_instant_metrics(
    client: PrometheusClient,
    queries: Dict[str, str],
    scheduler_name: str
) -> Dict[str, Any]:
    """Export instant metric values."""
    results = {
        "scheduler": scheduler_name,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "metrics": {}
    }
    
    for metric_name, query in queries.items():
        value = client.query(query)
        results["metrics"][metric_name] = value
        if value is not None:
            print(f"  {metric_name}: {value:.4f}")
        else:
            print(f"  {metric_name}: N/A")
    
    return results


def export_range_metrics(
    client: PrometheusClient,
    queries: Dict[str, str],
    duration_minutes: int = 5
) -> Dict[str, List[tuple]]:
    """Export time series metrics for a duration."""
    end = datetime.utcnow()
    start = end - timedelta(minutes=duration_minutes)
    
    results = {}
    for metric_name, query in queries.items():
        values = client.query_range(query, start, end)
        if values:
            results[metric_name] = values
            print(f"  {metric_name}: {len(values)} data points")
    
    return results


def save_results(results: Dict[str, Any], output_dir: str, scheduler_name: str):
    """Save results to JSON and CSV files."""
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    # Save JSON
    json_path = os.path.join(output_dir, f"{scheduler_name}_metrics.json")
    with open(json_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"✅ Saved JSON: {json_path}")
    
    # Save CSV summary
    csv_path = os.path.join(output_dir, f"{scheduler_name}_metrics_summary.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value", "unit"])
        
        metrics = results.get("metrics", {})
        units = {
            "millicores": ["cpu", "millicores"],
            "mb": ["memory", "mb"],
            "percent": ["percent", "utilization"],
            "seconds": ["latency", "duration"],
        }
        
        for metric_name, value in metrics.items():
            if value is None:
                continue
            
            # Determine unit
            unit = "value"
            metric_lower = metric_name.lower()
            for u, keywords in units.items():
                if any(k in metric_lower for k in keywords):
                    unit = u
                    break
            
            writer.writerow([metric_name, f"{value:.4f}" if value else "N/A", unit])
    
    print(f"✅ Saved CSV: {csv_path}")


def generate_comparison_table(data_dir: str, schedulers: List[str]) -> str:
    """Generate a markdown comparison table from exported metrics."""
    import os
    
    all_metrics = {}
    
    for scheduler in schedulers:
        json_path = os.path.join(data_dir, f"{scheduler}_metrics.json")
        if os.path.exists(json_path):
            with open(json_path, 'r') as f:
                data = json.load(f)
                all_metrics[scheduler] = data.get("metrics", {})
    
    if not all_metrics:
        return "No metrics found."
    
    # Get all metric names
    metric_names = set()
    for metrics in all_metrics.values():
        metric_names.update(metrics.keys())
    
    # Build table
    lines = ["| Metric | " + " | ".join(schedulers) + " |"]
    lines.append("|" + "---|" * (len(schedulers) + 1))
    
    for metric in sorted(metric_names):
        row = [metric]
        for scheduler in schedulers:
            value = all_metrics.get(scheduler, {}).get(metric)
            if value is not None:
                row.append(f"{value:.4f}")
            else:
                row.append("N/A")
        lines.append("| " + " | ".join(row) + " |")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Export Prometheus metrics for scheduler research")
    parser.add_argument("--prometheus", type=str, default="http://localhost:9090", help="Prometheus URL")
    parser.add_argument("--scheduler", type=str, required=True, help="Scheduler name (default, volcano, nexus)")
    parser.add_argument("--output", type=str, default="data", help="Output directory")
    parser.add_argument("--duration", type=int, default=5, help="Range query duration in minutes")
    parser.add_argument("--compare", nargs="+", help="Generate comparison table for these schedulers")
    
    args = parser.parse_args()
    
    if args.compare:
        print("\n📊 Generating Comparison Table")
        print("=" * 50)
        table = generate_comparison_table(args.output, args.compare)
        print(table)
        
        # Save comparison
        import os
        comparison_path = os.path.join(args.output, "scheduler_comparison.md")
        with open(comparison_path, 'w') as f:
            f.write("# Scheduler Comparison\n\n")
            f.write(table)
        print(f"\n✅ Saved comparison: {comparison_path}")
        return
    
    client = PrometheusClient(args.prometheus)
    
    print(f"\n📡 Connecting to Prometheus: {args.prometheus}")
    print(f"📋 Scheduler: {args.scheduler}")
    print("=" * 50)
    
    # Determine which queries to use
    queries = METRICS_QUERIES.copy()
    
    if args.scheduler.lower() == "volcano":
        queries.update(VOLCANO_QUERIES)
        print("Including Volcano-specific metrics")
    elif args.scheduler.lower() == "nexus":
        queries.update(NEXUS_QUERIES)
        print("Including NEXUS-specific metrics")
    
    # Export metrics
    print("\n📊 Exporting instant metrics...")
    results = export_instant_metrics(client, queries, args.scheduler)
    
    # Save results
    print("\n💾 Saving results...")
    save_results(results, args.output, args.scheduler)
    
    print("\n✅ Export complete!")


if __name__ == "__main__":
    main()
