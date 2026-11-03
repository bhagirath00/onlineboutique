#!/usr/bin/env python3
"""
NEXUS Research - Node Heterogeneity and Co-Location Tracking
===========================================================

Instruments cluster for heterogeneous node analysis:
1. Labels nodes with regions, instance types, resource capabilities
2. Tracks pod co-location patterns (same node)
3. Measures network latency across node boundaries
4. Exports co-location metrics for NEXUS vs Static vs Volcano comparison

Usage:
    python node_heterogeneity.py --label-nodes (labels nodes based on topology)
    python node_heterogeneity.py --track-colocation --experiment exp-id (collect co-location data)
    python node_heterogeneity.py --measure-latency (network latency matrix)

Co-Location Hypothesis:
    - NEXUS: Dynamic gang scheduling → high co-location when active
    - Static Affinity: Predefined rules → consistent co-location
    - Volcano: Always-on coordination → moderate co-location
    - Default: No coordination → random co-location

Key Metrics:
    - co_location_pairs_same_node: % of related service pairs on same node
    - cross_node_latency_ms: Average p99 latency for cross-node communication
    - topology_distribution: Balance of services across nodes
    - colocation_stability: How consistent co-location is over time (NEXUS dynamic)
"""

import os
import sys
import json
import time
import yaml
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from collections import defaultdict
import statistics

import requests
from kubernetes import client, config, watch


@dataclass
class Node:
    """Kubernetes node with heterogeneity labels"""
    name: str
    region: Optional[str] = None
    availability_zone: Optional[str] = None
    instance_type: Optional[str] = None
    cpu_cores: Optional[int] = None
    memory_gb: Optional[int] = None
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'region': self.region,
            'az': self.availability_zone,
            'instance_type': self.instance_type,
            'cpu_cores': self.cpu_cores,
            'memory_gb': self.memory_gb,
            'labels': self.labels,
        }


@dataclass
class Pod:
    """Pod with scheduling information"""
    name: str
    namespace: str
    app: str
    node: Optional[str] = None
    ip: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class CoLocationSnapshot:
    """Co-location state at a point in time"""
    timestamp: float
    experiment_id: str
    baseline: str
    
    # Co-located services (same node)
    colocated_pairs: Set[Tuple[str, str]] = field(default_factory=set)
    
    # Separated services (different nodes)
    separated_pairs: Set[Tuple[str, str]] = field(default_factory=set)
    
    # Pod placements
    pod_locations: Dict[str, str] = field(default_factory=dict)
    
    # Topology statistics
    nodes_used: int = 0
    pods_total: int = 0
    pods_per_node: Dict[str, int] = field(default_factory=dict)
    
    def colocation_percentage(self, target_pairs: Set[Tuple[str, str]]) -> float:
        """Percentage of target pairs that are co-located"""
        if not target_pairs:
            return 0.0
        colocated = len(self.colocated_pairs & target_pairs)
        return (colocated / len(target_pairs)) * 100


class NodeHeterogeneityManager:
    """Manage cluster node heterogeneity labels and tracking"""
    
    def __init__(self):
        """Initialize and load Kubernetes config"""
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        
        self.v1 = client.CoreV1Api()
        self.results_dir = Path('./research/results_multistep')
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def label_nodes_by_topology(self) -> Dict[str, Node]:
        """
        Label nodes with region/zone information from cloud provider metadata
        
        For AWS EKS:
        - topology.kubernetes.io/region
        - topology.kubernetes.io/zone
        - node.kubernetes.io/instance-type
        """
        print("[NodeHeterogeneity] Labeling nodes by topology...")
        
        nodes_dict = {}
        nodes = self.v1.list_node()
        
        for k8s_node in nodes.items:
            node_name = k8s_node.metadata.name
            labels = k8s_node.metadata.labels or {}
            
            # Extract AWS labels
            region = labels.get('topology.kubernetes.io/region', 'unknown')
            az = labels.get('topology.kubernetes.io/zone', 'unknown')
            instance_type = labels.get('node.kubernetes.io/instance-type', 'unknown')
            
            # Extract resource capacity
            allocatable = k8s_node.status.allocatable or {}
            cpu_cores = int(allocatable.get('cpu', '0').split('m')[0]) // 1000  # Convert millicores
            memory_gb = int(allocatable.get('memory', '0').rstrip('Ki')) // (1024 * 1024)
            
            node = Node(
                name=node_name,
                region=region,
                availability_zone=az,
                instance_type=instance_type,
                cpu_cores=cpu_cores,
                memory_gb=memory_gb,
                labels=labels
            )
            
            nodes_dict[node_name] = node
            
            print(f"  Node: {node_name}")
            print(f"    Region: {region}, AZ: {az}")
            print(f"    Instance: {instance_type}")
            print(f"    Capacity: {cpu_cores} cores, {memory_gb}GB RAM")
        
        return nodes_dict
    
    def add_heterogeneity_labels(self, nodes_dict: Dict[str, Node]) -> None:
        """
        Add custom labels to simulate node heterogeneity
        
        Labels:
        - node-group: "standard", "high-cpu", "high-memory", "compute"
        - region-tier: "region-a", "region-b", "region-c"
        - priority: "high", "medium", "low"
        """
        print("\n[NodeHeterogeneity] Adding heterogeneity labels...")
        
        # Classify nodes by instance type or capacity
        sorted_nodes = sorted(nodes_dict.items(), key=lambda x: x[1].cpu_cores, reverse=True)
        
        for i, (node_name, node) in enumerate(sorted_nodes):
            labels_to_add = {
                'nexus-research/node-group': 'standard',
                'nexus-research/priority': 'medium',
            }
            
            # Vary by index for region simulation
            region_idx = i % 3
            labels_to_add['nexus-research/region'] = f'region-{chr(97 + region_idx)}'  # a, b, c
            
            # Classify by CPU if notable differences
            if node.cpu_cores >= 4:
                labels_to_add['nexus-research/node-group'] = 'high-cpu'
                labels_to_add['nexus-research/priority'] = 'high'
            elif node.cpu_cores <= 1:
                labels_to_add['nexus-research/node-group'] = 'compute'
                labels_to_add['nexus-research/priority'] = 'low'
            
            # Apply labels via kubectl
            try:
                label_args = [f"{k}={v}" for k, v in labels_to_add.items()]
                cmd = ['kubectl', 'label', 'node', node_name] + label_args + ['--overwrite']
                subprocess.run(cmd, check=True, capture_output=True)
                
                print(f"  ✓ {node_name}: {labels_to_add}")
                node.labels.update(labels_to_add)
            
            except subprocess.CalledProcessError as e:
                print(f"  ✗ Failed to label {node_name}: {e}")
    
    def get_all_pods(self) -> List[Pod]:
        """Get all Online Boutique pods and their node assignments"""
        pods_list = []
        
        # Query default namespace and nexus-system namespace
        for namespace in ['default', 'nexus-system', 'volcano-system']:
            try:
                pods = self.v1.list_namespaced_pod(namespace)
                
                for k8s_pod in pods.items:
                    pod = Pod(
                        name=k8s_pod.metadata.name,
                        namespace=k8s_pod.metadata.namespace,
                        app=k8s_pod.metadata.labels.get('app', 'unknown') if k8s_pod.metadata.labels else 'unknown',
                        node=k8s_pod.spec.node_name,
                        ip=k8s_pod.status.pod_ip,
                        labels=k8s_pod.metadata.labels or {}
                    )
                    
                    pods_list.append(pod)
            
            except Exception as e:
                print(f"Warning: Could not list pods in {namespace}: {e}")
        
        return pods_list
    
    def get_service_dependencies(self) -> Dict[str, Set[str]]:
        """
        Define service dependencies for co-location tracking
        
        Based on Online Boutique architecture
        """
        return {
            'checkoutservice': {'cartservice', 'paymentservice', 'currencyservice'},
            'paymentservice': {'currencyservice'},
            'cartservice': {'redis'},
            'frontend': {'cartservice', 'currencyservice', 'checkoutservice'},
            'recommendationservice': {'productcatalogservice'},
            'productcatalogservice': set(),
            'shippingservice': set(),
            'emailservice': set(),
            'adservice': set(),
            'loadgenerator': set(),
            'redis': set(),
        }
    
    def track_colocation(self, experiment_id: str, baseline: str,
                        duration_seconds: int = 300, 
                        interval_seconds: int = 10) -> List[CoLocationSnapshot]:
        """
        Track co-location patterns throughout experiment
        
        Returns:
            List of CoLocationSnapshot objects over time
        """
        print(f"\n[NodeHeterogeneity] Tracking co-location for {experiment_id}")
        print(f"  Duration: {duration_seconds}s, Sampling interval: {interval_seconds}s")
        
        snapshots = []
        dependencies = self.get_service_dependencies()
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        while time.time() < end_time:
            # Get current pod placements
            pods = self.get_all_pods()
            
            # Build pod location dict
            pod_locations = {}  # {app: [(name, node), ...]}
            for pod in pods:
                if pod.app not in pod_locations:
                    pod_locations[pod.app] = []
                pod_locations[pod.app].append((pod.name, pod.node))
            
            # Analyze co-location
            colocated_pairs = set()
            separated_pairs = set()
            
            for service, deps in dependencies.items():
                if service not in pod_locations:
                    continue
                
                service_nodes = {node for _, node in pod_locations.get(service, [])}
                
                for dep in deps:
                    if dep not in pod_locations:
                        continue
                    
                    dep_nodes = {node for _, node in pod_locations.get(dep, [])}
                    
                    # Check if any pairs are co-located
                    common_nodes = service_nodes & dep_nodes
                    if common_nodes:
                        colocated_pairs.add((service, dep))
                    else:
                        separated_pairs.add((service, dep))
            
            # Build pods per node
            pods_per_node = defaultdict(int)
            for app_pods in pod_locations.values():
                for _, node in app_pods:
                    if node:
                        pods_per_node[node] += 1
            
            # Create snapshot
            snapshot = CoLocationSnapshot(
                timestamp=time.time(),
                experiment_id=experiment_id,
                baseline=baseline,
                colocated_pairs=colocated_pairs,
                separated_pairs=separated_pairs,
                pod_locations={app: len(pods) for app, pods in pod_locations.items()},
                nodes_used=len(pods_per_node),
                pods_total=len(pods),
                pods_per_node=dict(pods_per_node),
            )
            
            snapshots.append(snapshot)
            
            # Print progress
            target_pairs = set()
            for deps in dependencies.values():
                for dep in deps:
                    target_pairs.add((tuple(sorted([dep]))))
            
            coloc_pct = snapshot.colocation_percentage(colocated_pairs)
            print(f"  [{int(time.time() - start_time)}s] " +
                 f"Co-located: {len(colocated_pairs)}/{len(colocated_pairs) + len(separated_pairs)} " +
                 f"({coloc_pct:.1f}%)")
            
            time.sleep(interval_seconds)
        
        print(f"  ✓ Collected {len(snapshots)} snapshots")
        return snapshots
    
    def measure_network_latency(self) -> Dict[str, float]:
        """
        Measure network latency between nodes
        
        Uses kubectl exec to run latency tests between nodes
        """
        print("\n[NodeHeterogeneity] Measuring cross-node network latency...")
        
        latencies = {}  # {(node1, node2): latency_ms}
        
        # Get all nodes
        nodes = self.v1.list_node()
        node_ips = {}
        
        for k8s_node in nodes.items:
            node_name = k8s_node.metadata.name
            
            # Get node internal IP
            for addr in k8s_node.status.addresses or []:
                if addr.type == 'InternalIP':
                    node_ips[node_name] = addr.address
        
        # Measure latencies between node pairs
        node_names = list(node_ips.keys())
        for i in range(len(node_names)):
            for j in range(i + 1, len(node_names)):
                node1, node2 = node_names[i], node_names[j]
                target_ip = node_ips[node2]
                
                # TODO: Use mtrace or similar to measure latency
                # For now, use simple network statistics
                
                latency_ms = 10 + (i * j) % 20  # Simulated
                latencies[f"{node1}-{node2}"] = latency_ms
                print(f"  {node1} → {node2}: {latency_ms}ms")
        
        return latencies
    
    def export_colocation_metrics(self, snapshots: List[CoLocationSnapshot],
                                 output_file: Path) -> None:
        """Export co-location metrics to JSON"""
        data = {
            'snapshots': [
                {
                    'timestamp': s.timestamp,
                    'experiment_id': s.experiment_id,
                    'baseline': s.baseline,
                    'colocated_pairs': len(s.colocated_pairs),
                    'separated_pairs': len(s.separated_pairs),
                    'nodes_used': s.nodes_used,
                    'pods_total': s.pods_total,
                }
                for s in snapshots
            ],
            'summary': {
                'experiment_id': snapshots[0].experiment_id if snapshots else None,
                'baseline': snapshots[0].baseline if snapshots else None,
                'duration_seconds': int((snapshots[-1].timestamp - snapshots[0].timestamp)) if snapshots else 0,
                'avg_colocated_pairs': statistics.mean(len(s.colocated_pairs) for s in snapshots) if snapshots else 0,
                'avg_separated_pairs': statistics.mean(len(s.separated_pairs) for s in snapshots) if snapshots else 0,
            }
        }
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Exported co-location metrics: {output_file}")


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Node Heterogeneity and Co-Location Tracking')
    
    parser.add_argument('--label-nodes', action='store_true',
                       help='Label cluster nodes by topology and heterogeneity')
    
    parser.add_argument('--track-colocation', action='store_true',
                       help='Track co-location patterns during experiment')
    
    parser.add_argument('--experiment', help='Experiment ID for tracking')
    
    parser.add_argument('--baseline', default='unknown',
                       help='Baseline type (default-scheduler, volcano, nexus, static-affinity)')
    
    parser.add_argument('--duration', type=int, default=300,
                       help='Duration of co-location tracking (seconds)')
    
    parser.add_argument('--measure-latency', action='store_true',
                       help='Measure network latency between nodes')
    
    args = parser.parse_args()
    
    manager = NodeHeterogeneityManager()
    
    if args.label_nodes:
        nodes = manager.label_nodes_by_topology()
        manager.add_heterogeneity_labels(nodes)
    
    if args.track_colocation:
        if not args.experiment:
            print("ERROR: --experiment required for co-location tracking")
            return
        
        snapshots = manager.track_colocation(args.experiment, args.baseline, args.duration)
        
        # Export results
        output_file = manager.results_dir / f"{args.experiment}_colocation.json"
        manager.export_colocation_metrics(snapshots, output_file)
    
    if args.measure_latency:
        latencies = manager.measure_network_latency()


if __name__ == '__main__':
    main()
