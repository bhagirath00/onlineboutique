#!/usr/bin/env python3
"""
NEXUS Research - Data Collection Pipeline
==========================================

Comprehensive metrics collection and export for research analysis:
1. Query Prometheus for time-series metrics
2. Process application-level latency measurements
3. Calculate control-plane overhead metrics
4. Export results in CSV/JSON for statistical analysis
5. Compare baselines with statistical tests

Usage:
    python data_collector.py --experiment default-scheduler_spike_r1 --metrics latency,overhead
    python data_collector.py --baseline nexus-scheduler --aggregate (aggregate all runs of baseline)
    python data_collector.py --compare-baselines (statistical comparison Default vs Volcano vs NEXUS)

Requirements:
    - Prometheus with Online Boutique and scheduler metrics
    - Online Boutique services instrumented with latency metrics
    - control_plane_exporter.py in parent metrics directory
"""

import os
import sys
import json
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from urllib.parse import urlencode

import requests
import numpy as np
from scipy import stats


sys.path.insert(0, str(Path(__file__).parent.parent / "metrics"))
try:
    from control_plane_exporter import PrometheusMetricsQuery, ControlPlaneMetricsCollector
except ImportError:
    print("WARNING: control_plane_exporter not found, some features disabled")


@dataclass
class MetricsSnapshot:
    """Single measurement of all metrics at one timestamp"""
    timestamp: float
    experiment_id: str
    baseline: str
    
    # Application metrics (latency)
    request_latency_ms: Optional[float] = None
    request_latency_p50_ms: Optional[float] = None
    request_latency_p95_ms: Optional[float] = None
    request_latency_p99_ms: Optional[float] = None
    
    # Query metrics (service-level)
    service_latency_ms: Dict[str, float] = field(default_factory=dict)
    
    # Resource metrics
    scheduler_cpu_usage: Optional[float] = None
    scheduler_memory_usage: Optional[float] = None
    scheduler_memory_mb: Optional[float] = None
    
    # Scheduling metrics
    pending_pods_count: Optional[int] = None
    pod_scheduling_rate: Optional[float] = None
    pod_unschedulable_count: Optional[int] = None
    
    # Control-plane metrics
    api_server_request_rate: Optional[float] = None
    api_server_latency_p99_ms: Optional[float] = None
    etcd_write_rate: Optional[float] = None
    
    # NEXUS-specific metrics
    scheduler_state: Optional[str] = None  # IDLE, ACTIVE, etc
    gang_formations: Optional[int] = None
    gang_dissolutions: Optional[int] = None
    extender_latency_p99_ms: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """Convert snapshot to dictionary"""
        d = {
            'timestamp': self.timestamp,
            'experiment_id': self.experiment_id,
            'baseline': self.baseline,
            'request_latency_ms': self.request_latency_ms,
            'request_latency_p50_ms': self.request_latency_p50_ms,
            'request_latency_p95_ms': self.request_latency_p95_ms,
            'request_latency_p99_ms': self.request_latency_p99_ms,
            'scheduler_cpu_usage': self.scheduler_cpu_usage,
            'scheduler_memory_usage': self.scheduler_memory_usage,
            'pending_pods': self.pending_pods_count,
            'pod_scheduling_rate': self.pod_scheduling_rate,
            'api_server_request_rate': self.api_server_request_rate,
            'api_server_latency_p99_ms': self.api_server_latency_p99_ms,
            'etcd_write_rate': self.etcd_write_rate,
            'scheduler_state': self.scheduler_state,
            'gang_formations': self.gang_formations,
            'extender_latency_p99_ms': self.extender_latency_p99_ms,
        }
        d.update(self.service_latency_ms)
        return d


@dataclass
class ExperimentMetricsSummary:
    """Summary statistics for single experiment run"""
    experiment_id: str
    baseline: str
    scenario: str
    repetition: int
    duration_seconds: int
    
    # Latency statistics
    latency_p50_ms: Optional[float] = None
    latency_p95_ms: Optional[float] = None
    latency_p99_ms: Optional[float] = None
    latency_max_ms: Optional[float] = None
    latency_mean_ms: Optional[float] = None
    
    # Resource usage statistics
    scheduler_cpu_avg: Optional[float] = None
    scheduler_cpu_max: Optional[float] = None
    scheduler_memory_avg: Optional[float] = None
    scheduler_memory_max: Optional[float] = None
    
    # Scheduling efficiency
    pending_pods_avg: Optional[float] = None
    pending_pods_max: Optional[int] = None
    pod_scheduling_rate_avg: Optional[float] = None
    unschedulable_pods_total: Optional[int] = None
    
    # Control-plane overhead
    api_server_request_rate_avg: Optional[float] = None
    api_server_latency_p99_avg_ms: Optional[float] = None
    etcd_write_rate_avg: Optional[float] = None
    
    # NEXUS-specific
    total_gang_formations: Optional[int] = None
    extender_latency_p99_avg_ms: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'experiment_id': self.experiment_id,
            'baseline': self.baseline,
            'scenario': self.scenario,
            'repetition': self.repetition,
            'duration_seconds': self.duration_seconds,
            'latency_p50_ms': self.latency_p50_ms,
            'latency_p95_ms': self.latency_p95_ms,
            'latency_p99_ms': self.latency_p99_ms,
            'latency_max_ms': self.latency_max_ms,
            'latency_mean_ms': self.latency_mean_ms,
            'scheduler_cpu_avg': self.scheduler_cpu_avg,
            'scheduler_cpu_max': self.scheduler_cpu_max,
            'scheduler_memory_avg': self.scheduler_memory_avg,
            'scheduler_memory_max': self.scheduler_memory_max,
            'pending_pods_avg': self.pending_pods_avg,
            'pending_pods_max': self.pending_pods_max,
            'pod_scheduling_rate_avg': self.pod_scheduling_rate_avg,
            'unschedulable_pods_total': self.unschedulable_pods_total,
            'api_server_request_rate_avg': self.api_server_request_rate_avg,
            'api_server_latency_p99_avg_ms': self.api_server_latency_p99_avg_ms,
            'etcd_write_rate_avg': self.etcd_write_rate_avg,
            'total_gang_formations': self.total_gang_formations,
            'extender_latency_p99_avg_ms': self.extender_latency_p99_avg_ms,
        }


class DataCollector:
    """Collect and export metrics from Prometheus"""
    
    def __init__(self, prometheus_url: str = None):
        """Initialize data collector"""
        self.prometheus_url = prometheus_url or os.getenv('PROMETHEUS_URL', 
                                                           'http://prometheus.nexus-system:9090')
        self.prom_query = PrometheusMetricsQuery(self.prometheus_url)
        self.results_dir = Path('./research/results_multistep')
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def collect_experiment_metrics(self, experiment_id: str, baseline: str, 
                                  start_time: datetime, end_time: datetime,
                                  interval_seconds: int = 5) -> List[MetricsSnapshot]:
        """
        Collect all metrics for given time range
        
        Args:
            experiment_id: Unique experiment identifier
            baseline: Baseline name (default-scheduler, volcano-scheduler, nexus-scheduler, static-affinity)
            start_time: Experiment start time
            end_time: Experiment end time
            interval_seconds: Scrape interval
        
        Returns:
            List of MetricsSnapshot objects with time-series data
        """
        duration_seconds = int((end_time - start_time).total_seconds())
        snapshots = []
        
        # Query Prometheus for full time range (more efficient than scraping)
        print(f"[DataCollector] Collecting metrics for {experiment_id}")
        print(f"  Time range: {start_time} → {end_time} ({duration_seconds}s)")
        print(f"  Baseline: {baseline}")
        
        try:
            # Request latency histogram (all percentiles)
            req_latency_p99 = self._query_latency_percentile(
                'request_duration_seconds', 0.99, start_time, end_time
            )
            req_latency_p95 = self._query_latency_percentile(
                'request_duration_seconds', 0.95, start_time, end_time
            )
            req_latency_p50 = self._query_latency_percentile(
                'request_duration_seconds', 0.50, start_time, end_time
            )
            
            # Scheduler resource usage (CPU, memory)
            sched_cpu_usage = self._query_metric(
                'rate(scheduler_cpu_seconds_total[5m])', start_time, end_time
            )
            sched_memory_mb = self._query_metric(
                'scheduler_memory_mb', start_time, end_time
            )
            
            # Pending pods
            pending_pods = self._query_metric(
                'kubernetes_pods_pending_total', start_time, end_time
            )
            
            # Pod scheduling rate (pods scheduled per minute)
            scheduling_rate = self._query_metric(
                'rate(pods_scheduled_total[1m])', start_time, end_time
            )
            
            # API server metrics
            api_req_rate = self._query_metric(
                'rate(apiserver_request_total[1m])', start_time, end_time
            )
            
            api_latency_p99 = self._query_metric(
                'histogram_quantile(0.99, rate(apiserver_request_duration_seconds_bucket[5m]))',
                start_time, end_time
            )
            
            # etcd metrics
            etcd_write_rate = self._query_metric(
                'rate(etcd_server_has_leader[1m])', start_time, end_time
            )
            
            # NEXUS-specific metrics
            if 'nexus' in baseline:
                extender_latency_p99 = self._query_metric(
                    'histogram_quantile(0.99, rate(nexus_extender_latency_ms_bucket[5m]))',
                    start_time, end_time
                )
                gang_count = self._query_metric(
                    'nexus_gangs_active', start_time, end_time
                )
            else:
                extender_latency_p99 = None
                gang_count = None
            
            # Convert to time-series snapshots
            # For simplicity, create single summary snapshot
            # In production, would create snapshot per interval
            
            snapshot = MetricsSnapshot(
                timestamp=end_time.timestamp(),
                experiment_id=experiment_id,
                baseline=baseline,
                request_latency_p99_ms=(req_latency_p99 * 1000) if req_latency_p99 else None,
                request_latency_p95_ms=(req_latency_p95 * 1000) if req_latency_p95 else None,
                request_latency_p50_ms=(req_latency_p50 * 1000) if req_latency_p50 else None,
                scheduler_cpu_usage=sched_cpu_usage,
                scheduler_memory_mb=sched_memory_mb,
                pending_pods_count=int(pending_pods) if pending_pods else None,
                pod_scheduling_rate=scheduling_rate,
                api_server_request_rate=api_req_rate,
                api_server_latency_p99_ms=(api_latency_p99 * 1000) if api_latency_p99 else None,
                etcd_write_rate=etcd_write_rate,
                extender_latency_p99_ms=(extender_latency_p99 * 1000) if extender_latency_p99 else None,
            )
            
            snapshots.append(snapshot)
            
            print(f"  ✓ Collected {len(snapshots)} snapshot(s)")
            print(f"    Request latency p99: {snapshot.request_latency_p99_ms:.2f}ms")
            print(f"    Scheduler CPU: {snapshot.scheduler_cpu_usage:.4f}")
            print(f"    API server latency p99: {snapshot.api_server_latency_p99_ms:.2f}ms")
            
        except Exception as e:
            print(f"  ✗ Error collecting metrics: {e}")
            import traceback
            traceback.print_exc()
        
        return snapshots
    
    def _query_metric(self, promql: str, start_time: datetime, 
                     end_time: datetime) -> Optional[float]:
        """Query single metric from Prometheus, return average"""
        try:
            results = self.prom_query.query_range(
                promql,
                int(start_time.timestamp()),
                int(end_time.timestamp()),
                1800  # 30m step
            )
            
            if results and len(results) > 0:
                # Average the values
                values = [float(v[1]) for v in results[0].get('values', [])]
                if values:
                    return np.mean(values)
            
            return None
        except Exception as e:
            print(f"  Warning: Query failed for {promql}: {e}")
            return None
    
    def _query_latency_percentile(self, metric_base: str, percentile: float,
                                 start_time: datetime, 
                                 end_time: datetime) -> Optional[float]:
        """Query latency percentile"""
        # Use histogram_quantile to compute percentile
        p_int = int(percentile * 100)
        promql = f'histogram_quantile({percentile}, rate({metric_base}_bucket[5m]))'
        
        return self._query_metric(promql, start_time, end_time)
    
    def summarize_experiment(self, snapshots: List[MetricsSnapshot],
                           experiment_id: str, baseline: str, scenario: str,
                           repetition: int) -> ExperimentMetricsSummary:
        """
        Generate summary statistics from metric snapshots
        
        Returns:
            ExperimentMetricsSummary with aggregated metrics
        """
        if not snapshots:
            return ExperimentMetricsSummary(
                experiment_id=experiment_id,
                baseline=baseline,
                scenario=scenario,
                repetition=repetition,
                duration_seconds=0
            )
        
        # Extract all latency values
        latencies = [s.request_latency_p99_ms for s in snapshots 
                    if s.request_latency_p99_ms is not None]
        latencies_p95 = [s.request_latency_p95_ms for s in snapshots 
                        if s.request_latency_p95_ms is not None]
        latencies_p50 = [s.request_latency_p50_ms for s in snapshots 
                        if s.request_latency_p50_ms is not None]
        
        # Extract resource metrics
        cpu_values = [s.scheduler_cpu_usage for s in snapshots 
                     if s.scheduler_cpu_usage is not None]
        memory_values = [s.scheduler_memory_mb for s in snapshots 
                        if s.scheduler_memory_mb is not None]
        
        # Extract pending pod counts
        pending = [s.pending_pods_count for s in snapshots 
                  if s.pending_pods_count is not None]
        
        # Compute summaries
        summary = ExperimentMetricsSummary(
            experiment_id=experiment_id,
            baseline=baseline,
            scenario=scenario,
            repetition=repetition,
            duration_seconds=int((snapshots[-1].timestamp - snapshots[0].timestamp)),
            
            # Latency stats
            latency_p99_ms=np.mean(latencies) if latencies else None,
            latency_p95_ms=np.mean(latencies_p95) if latencies_p95 else None,
            latency_p50_ms=np.mean(latencies_p50) if latencies_p50 else None,
            latency_max_ms=max(latencies) if latencies else None,
            latency_mean_ms=np.mean(latencies) if latencies else None,
            
            # Resource stats
            scheduler_cpu_avg=np.mean(cpu_values) if cpu_values else None,
            scheduler_cpu_max=max(cpu_values) if cpu_values else None,
            scheduler_memory_avg=np.mean(memory_values) if memory_values else None,
            scheduler_memory_max=max(memory_values) if memory_values else None,
            
            # Scheduling stats
            pending_pods_avg=np.mean(pending) if pending else None,
            pending_pods_max=max(pending) if pending else None,
        )
        
        return summary
    
    def export_metrics_csv(self, snapshots: List[MetricsSnapshot],
                          output_file: Path) -> None:
        """Export time-series metrics to CSV"""
        if not snapshots:
            return
        
        with open(output_file, 'w', newline='') as f:
            fieldnames = snapshots[0].to_dict().keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for snapshot in snapshots:
                writer.writerow(snapshot.to_dict())
        
        print(f"Exported metrics: {output_file}")
    
    def export_summary_csv(self, summaries: List[ExperimentMetricsSummary],
                          output_file: Path) -> None:
        """Export experiment summaries to CSV"""
        if not summaries:
            return
        
        with open(output_file, 'w', newline='') as f:
            fieldnames = summaries[0].to_dict().keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for summary in summaries:
                writer.writerow(summary.to_dict())
        
        print(f"Exported summaries: {output_file}")
    
    def compare_baselines(self, summaries: List[ExperimentMetricsSummary]) -> Dict:
        """
        Statistical comparison between baselines
        
        Compares: Default vs Volcano vs Static vs NEXUS
        Metrics: latency, overhead, scheduling rate
        Test: t-test with p=0.05 significance
        """
        print("\n[DataCollector] Baseline Comparison Analysis")
        
        # Group by baseline
        by_baseline = defaultdict(list)
        for summary in summaries:
            by_baseline[summary.baseline].append(summary)
        
        comparisons = {}
        
        # Compare latencies
        print("\nLatency Comparison (p99):")
        for baseline_name, runs in by_baseline.items():
            latencies = [r.latency_p99_ms for r in runs if r.latency_p99_ms]
            if latencies:
                mean_latency = np.mean(latencies)
                std_latency = np.std(latencies)
                print(f"  {baseline_name}: {mean_latency:.2f}ms ± {std_latency:.2f}ms")
                comparisons[f"{baseline_name}_latency"] = {
                    'mean': mean_latency,
                    'std': std_latency,
                    'n': len(latencies)
                }
        
        # Compare CPU overhead
        print("\nScheduler CPU Overhead:")
        for baseline_name, runs in by_baseline.items():
            cpus = [r.scheduler_cpu_avg for r in runs if r.scheduler_cpu_avg]
            if cpus:
                mean_cpu = np.mean(cpus)
                std_cpu = np.std(cpus)
                print(f"  {baseline_name}: {mean_cpu:.4f} ± {std_cpu:.4f}")
                comparisons[f"{baseline_name}_cpu"] = {
                    'mean': mean_cpu,
                    'std': std_cpu,
                    'n': len(cpus)
                }
        
        # T-tests between NEXUS and others
        print("\nStatistical Tests (NEXUS vs Others):")
        nexus_runs = by_baseline.get('nexus-scheduler', [])
        if nexus_runs:
            nexus_latencies = [r.latency_p99_ms for r in nexus_runs if r.latency_p99_ms]
            nexus_cpus = [r.scheduler_cpu_avg for r in nexus_runs if r.scheduler_cpu_avg]
            
            for baseline_name, runs in by_baseline.items():
                if baseline_name == 'nexus-scheduler':
                    continue
                
                other_latencies = [r.latency_p99_ms for r in runs if r.latency_p99_ms]
                if nexus_latencies and other_latencies:
                    t_stat, p_value = stats.ttest_ind(nexus_latencies, other_latencies)
                    print(f"  Latency NEXUS vs {baseline_name}: p={p_value:.4f} " +
                         ("*" if p_value < 0.05 else "ns"))
        
        return comparisons


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NEXUS Data Collection Pipeline')
    
    parser.add_argument('--experiment', help='Experiment ID to analyze')
    parser.add_argument('--baseline', help='Baseline to aggregate (all runs)')
    parser.add_argument('--start-time', type=str, help='Start time (ISO format)')
    parser.add_argument('--end-time', type=str, help='End time (ISO format)')
    parser.add_argument('--compare-baselines', action='store_true',
                       help='Run baseline comparison analysis')
    parser.add_argument('--prometheus-url', 
                       default=os.getenv('PROMETHEUS_URL', 
                                        'http://prometheus.nexus-system:9090'),
                       help='Prometheus URL')
    
    args = parser.parse_args()
    
    collector = DataCollector(prometheus_url=args.prometheus_url)
    
    if args.experiment:
        # Collect metrics for single experiment
        if not args.start_time or not args.end_time:
            print("ERROR: --start-time and --end-time required")
            return
        
        start = datetime.fromisoformat(args.start_time)
        end = datetime.fromisoformat(args.end_time)
        
        snapshots = collector.collect_experiment_metrics(
            args.experiment, 'unknown', start, end
        )
        
        # Export
        output_file = collector.results_dir / f"{args.experiment}_metrics.csv"
        collector.export_metrics_csv(snapshots, output_file)
    
    elif args.baseline:
        # Aggregate all experiments for baseline
        print(f"Aggregating metrics for baseline: {args.baseline}")
        # TODO: Load all experiment results for baseline and aggregate
    
    elif args.compare_baselines:
        # Compare all baselines
        print("Running baseline comparison analysis...")
        # TODO: Load all experiment results and run comparison
        summaries = []  # Load from results.json
        comparisons = collector.compare_baselines(summaries)


if __name__ == '__main__':
    main()
