#!/usr/bin/env python3
"""
Gantt Chart Generator for Kubernetes Pod Scheduling
====================================================

This script generates Gantt charts showing pod scheduling timeline.
It extracts pod events from kubectl and visualizes the scheduling process.

Usage:
    python gantt_generator.py --events events.json --output gantt_chart.png
    
Or collect events directly:
    python gantt_generator.py --collect --namespace default --output gantt_chart.png
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.dates import DateFormatter, MinuteLocator
except ImportError:
    print("ERROR: matplotlib is required. Install with: pip install matplotlib")
    sys.exit(1)


class PodEvent:
    """Represents a pod scheduling event."""
    
    def __init__(self, name: str, namespace: str):
        self.name = name
        self.namespace = namespace
        self.created_at: Optional[datetime] = None
        self.scheduled_at: Optional[datetime] = None
        self.running_at: Optional[datetime] = None
        self.node: Optional[str] = None
        self.scheduler: str = "default-scheduler"
    
    @property
    def scheduling_duration(self) -> Optional[float]:
        """Time from creation to scheduling in seconds."""
        if self.created_at and self.scheduled_at:
            return (self.scheduled_at - self.created_at).total_seconds()
        return None
    
    @property
    def startup_duration(self) -> Optional[float]:
        """Time from scheduling to running in seconds."""
        if self.scheduled_at and self.running_at:
            return (self.running_at - self.scheduled_at).total_seconds()
        return None
    
    @property
    def total_duration(self) -> Optional[float]:
        """Total time from creation to running in seconds."""
        if self.created_at and self.running_at:
            return (self.running_at - self.created_at).total_seconds()
        return None


def parse_k8s_timestamp(ts: str) -> datetime:
    """Parse Kubernetes timestamp to datetime."""
    # Handle formats like: 2026-01-07T08:00:00Z
    if ts.endswith('Z'):
        ts = ts[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except:
        return datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")


def collect_events_from_kubectl(namespace: str = "default") -> dict:
    """Collect events directly from kubectl."""
    cmd = ["kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp", "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: kubectl failed: {e.stderr}")
        sys.exit(1)
    except json.JSONDecodeError:
        print("ERROR: Failed to parse kubectl output as JSON")
        sys.exit(1)


def collect_pods_from_kubectl(namespace: str = "default") -> dict:
    """Collect pod info directly from kubectl."""
    cmd = ["kubectl", "get", "pods", "-n", namespace, "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: kubectl failed: {e.stderr}")
        sys.exit(1)


def parse_events(events_data: dict, pods_data: Optional[dict] = None) -> Dict[str, PodEvent]:
    """Parse Kubernetes events into PodEvent objects."""
    pods: Dict[str, PodEvent] = {}
    
    # First, get pod info if available
    if pods_data:
        for pod in pods_data.get("items", []):
            name = pod["metadata"]["name"]
            namespace = pod["metadata"]["namespace"]
            pods[name] = PodEvent(name, namespace)
            
            # Get scheduler name
            pods[name].scheduler = pod["spec"].get("schedulerName", "default-scheduler")
            
            # Get node
            pods[name].node = pod["spec"].get("nodeName")
            
            # Get creation timestamp
            if "creationTimestamp" in pod["metadata"]:
                pods[name].created_at = parse_k8s_timestamp(pod["metadata"]["creationTimestamp"])
            
            # Parse conditions for running time
            for condition in pod.get("status", {}).get("conditions", []):
                if condition.get("type") == "Ready" and condition.get("status") == "True":
                    if "lastTransitionTime" in condition:
                        pods[name].running_at = parse_k8s_timestamp(condition["lastTransitionTime"])
    
    # Then parse events for scheduling info
    for event in events_data.get("items", []):
        involved = event.get("involvedObject", {})
        if involved.get("kind") != "Pod":
            continue
        
        pod_name = involved.get("name", "")
        namespace = involved.get("namespace", "default")
        
        if pod_name not in pods:
            pods[pod_name] = PodEvent(pod_name, namespace)
        
        reason = event.get("reason", "")
        timestamp = parse_k8s_timestamp(event.get("firstTimestamp") or event.get("eventTime", ""))
        
        if reason == "Scheduled":
            pods[pod_name].scheduled_at = timestamp
            # Extract node from message
            message = event.get("message", "")
            if "to" in message:
                node = message.split("to")[-1].strip()
                pods[pod_name].node = node
        elif reason == "Started":
            pods[pod_name].running_at = timestamp
        elif reason == "SuccessfulCreate":
            pods[pod_name].created_at = timestamp
    
    return pods


def generate_gantt_chart(
    pods: Dict[str, PodEvent],
    output_path: str,
    title: str = "Pod Scheduling Timeline",
    scheduler_name: str = ""
):
    """Generate a Gantt chart visualization."""
    
    # Filter pods with valid timeline data
    valid_pods = [p for p in pods.values() if p.created_at and (p.scheduled_at or p.running_at)]
    
    if not valid_pods:
        print("WARNING: No pods with valid timeline data found.")
        return
    
    # Sort by creation time
    valid_pods.sort(key=lambda p: p.created_at)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, max(6, len(valid_pods) * 0.4)))
    
    # Colors for different phases
    colors = {
        'pending': '#FFA500',      # Orange - waiting to be scheduled
        'scheduling': '#4169E1',   # Royal Blue - scheduling in progress
        'starting': '#32CD32',     # Lime Green - container starting
        'running': '#228B22',      # Forest Green - running
    }
    
    # Find time range
    min_time = min(p.created_at for p in valid_pods)
    max_time = max(p.running_at or p.scheduled_at or p.created_at for p in valid_pods)
    
    # Plot each pod
    y_positions = []
    y_labels = []
    
    for i, pod in enumerate(valid_pods):
        y = len(valid_pods) - i - 1
        y_positions.append(y)
        
        # Truncate pod name for label
        label = pod.name[:30] + "..." if len(pod.name) > 30 else pod.name
        y_labels.append(label)
        
        bar_height = 0.6
        
        # Phase 1: Pending (created -> scheduled)
        if pod.created_at and pod.scheduled_at:
            duration = (pod.scheduled_at - pod.created_at).total_seconds()
            ax.barh(y, duration, left=(pod.created_at - min_time).total_seconds(),
                   height=bar_height, color=colors['pending'], edgecolor='black', linewidth=0.5)
        
        # Phase 2: Starting (scheduled -> running)
        if pod.scheduled_at and pod.running_at:
            duration = (pod.running_at - pod.scheduled_at).total_seconds()
            ax.barh(y, duration, left=(pod.scheduled_at - min_time).total_seconds(),
                   height=bar_height, color=colors['starting'], edgecolor='black', linewidth=0.5)
        
        # Add node annotation
        if pod.node:
            node_short = pod.node.split(".")[0][-12:]  # Last 12 chars of hostname
            text_x = (pod.running_at or pod.scheduled_at or pod.created_at) - min_time
            ax.annotate(node_short, xy=(text_x.total_seconds() + 1, y),
                       fontsize=7, va='center', alpha=0.7)
    
    # Customize axes
    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels, fontsize=8)
    ax.set_xlabel('Time (seconds from first pod creation)', fontsize=10)
    ax.set_ylabel('Pod Name', fontsize=10)
    
    # Title
    full_title = title
    if scheduler_name:
        full_title += f" ({scheduler_name})"
    ax.set_title(full_title, fontsize=12, fontweight='bold')
    
    # Legend
    legend_patches = [
        mpatches.Patch(color=colors['pending'], label='Pending (waiting for scheduling)'),
        mpatches.Patch(color=colors['starting'], label='Starting (container startup)'),
    ]
    ax.legend(handles=legend_patches, loc='upper right', fontsize=8)
    
    # Grid
    ax.grid(axis='x', linestyle='--', alpha=0.3)
    ax.set_axisbelow(True)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Gantt chart saved to: {output_path}")
    
    # Print statistics
    print("\nScheduling Statistics:")
    print("-" * 50)
    
    scheduling_times = [p.scheduling_duration for p in valid_pods if p.scheduling_duration is not None]
    startup_times = [p.startup_duration for p in valid_pods if p.startup_duration is not None]
    total_times = [p.total_duration for p in valid_pods if p.total_duration is not None]
    
    if scheduling_times:
        print(f"Scheduling Latency (creation -> scheduled):")
        print(f"  Min: {min(scheduling_times):.2f}s | Max: {max(scheduling_times):.2f}s | Avg: {sum(scheduling_times)/len(scheduling_times):.2f}s")
    
    if startup_times:
        print(f"Container Startup (scheduled -> running):")
        print(f"  Min: {min(startup_times):.2f}s | Max: {max(startup_times):.2f}s | Avg: {sum(startup_times)/len(startup_times):.2f}s")
    
    if total_times:
        print(f"Total Pod Startup (creation -> running):")
        print(f"  Min: {min(total_times):.2f}s | Max: {max(total_times):.2f}s | Avg: {sum(total_times)/len(total_times):.2f}s")
    
    print(f"\nTotal pods analyzed: {len(valid_pods)}")
    
    # Return statistics for programmatic use
    return {
        "pod_count": len(valid_pods),
        "scheduling_latency": {
            "min": min(scheduling_times) if scheduling_times else None,
            "max": max(scheduling_times) if scheduling_times else None,
            "avg": sum(scheduling_times)/len(scheduling_times) if scheduling_times else None,
        },
        "startup_time": {
            "min": min(startup_times) if startup_times else None,
            "max": max(startup_times) if startup_times else None,
            "avg": sum(startup_times)/len(startup_times) if startup_times else None,
        },
        "total_time": {
            "min": min(total_times) if total_times else None,
            "max": max(total_times) if total_times else None,
            "avg": sum(total_times)/len(total_times) if total_times else None,
        }
    }


def main():
    parser = argparse.ArgumentParser(description="Generate Gantt charts for Kubernetes pod scheduling")
    parser.add_argument("--events", type=str, help="Path to events.json file")
    parser.add_argument("--pods", type=str, help="Path to pods.json file (optional)")
    parser.add_argument("--collect", action="store_true", help="Collect events directly from kubectl")
    parser.add_argument("--namespace", type=str, default="default", help="Kubernetes namespace")
    parser.add_argument("--output", type=str, default="gantt_chart.png", help="Output file path")
    parser.add_argument("--title", type=str, default="Pod Scheduling Timeline", help="Chart title")
    parser.add_argument("--scheduler", type=str, default="", help="Scheduler name for title")
    
    args = parser.parse_args()
    
    # Collect or load events
    if args.collect:
        print(f"Collecting events from namespace: {args.namespace}")
        events_data = collect_events_from_kubectl(args.namespace)
        pods_data = collect_pods_from_kubectl(args.namespace)
    elif args.events:
        print(f"Loading events from: {args.events}")
        with open(args.events, 'r') as f:
            events_data = json.load(f)
        pods_data = None
        if args.pods:
            with open(args.pods, 'r') as f:
                pods_data = json.load(f)
    else:
        print("ERROR: Either --events or --collect must be specified")
        parser.print_help()
        sys.exit(1)
    
    # Parse events
    pods = parse_events(events_data, pods_data)
    print(f"Found {len(pods)} pods")
    
    # Generate chart
    generate_gantt_chart(pods, args.output, args.title, args.scheduler)


if __name__ == "__main__":
    main()
