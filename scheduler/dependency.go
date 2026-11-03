/*
Dependency Graph Engine
======================
Builds an in-memory Directed Acyclic Graph (DAG) from pod annotations
at RUNTIME during spike events. The graph is ephemeral - it exists
only during the spike window and is cleared on dissolution.

Required by Section 2D of the professional review:
"Graph must not be static YAML-only. The graph must exist in
memory only during spike window."

Annotations used:
  nexus.io/depends-on: "paymentservice,currencyservice"
  nexus.io/service-group: "checkout-flow"

If no annotations are found, falls back to well-known Online Boutique
dependency patterns for the research experiment.
*/

package main

import (
	"context"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// Annotation keys for dependency declaration
	AnnotationDependsOn    = "nexus.io/depends-on"
	AnnotationServiceGroup = "nexus.io/service-group"
)

// RuntimeGroup represents a dynamically-discovered coordination group
type RuntimeGroup struct {
	Name     string
	Services []string
}

// DependencyGraph builds and holds the in-memory service DAG
type DependencyGraph struct {
	clientset *kubernetes.Clientset
	nodes     map[string][]string // Adjacency list: service -> list of dependencies
	groups    []RuntimeGroup      // Identified coordination groups
	built     bool
}

// NewDependencyGraph creates a new (empty) dependency graph
func NewDependencyGraph(clientset *kubernetes.Clientset) *DependencyGraph {
	return &DependencyGraph{
		clientset: clientset,
		nodes:     make(map[string][]string),
		built:     false,
	}
}

// BuildFromAnnotations scans all pods in the cluster for nexus.io annotations
// and constructs the dependency graph at runtime
func (dg *DependencyGraph) BuildFromAnnotations(ctx context.Context) error {
	klog.Info("Building dependency graph from pod annotations...")
	dg.nodes = make(map[string][]string)

	pods, err := dg.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, pod := range pods.Items {
		if pod.Annotations == nil {
			continue
		}

		serviceName := extractServiceName(pod.Name)
		if depsStr, ok := pod.Annotations[AnnotationDependsOn]; ok {
			deps := strings.Split(depsStr, ",")
			for _, dep := range deps {
				dep = strings.TrimSpace(dep)
				if dep != "" {
					dg.nodes[serviceName] = append(dg.nodes[serviceName], dep)
				}
			}
		}
	}

	if len(dg.nodes) == 0 {
		klog.Info("No annotations found, using well-known Online Boutique critical path")
		dg.loadExperimentDefaults()
	}

	dg.built = true
	return nil
}

// GetGroups returns the critical path as a RuntimeGroup
// Implements Stage 3: Critical Path Identification
func (dg *DependencyGraph) GetGroups() []RuntimeGroup {
	if !dg.built || len(dg.nodes) == 0 {
		return nil
	}

	// If already identified, return them
	if len(dg.groups) > 0 {
		return dg.groups
	}

	// 1. Find the longest path (critical path) in the DAG
	path := dg.findLongestPath()
	if len(path) == 0 {
		return nil
	}

	klog.Infof("Critical path identified: %v", path)
	dg.groups = []RuntimeGroup{
		{
			Name:     "critical-path",
			Services: path,
		},
	}
	return dg.groups
}

// findLongestPath implements the longest-path algorithm for a DAG
func (dg *DependencyGraph) findLongestPath() []string {
	// Simple longest path on DAG using topological sort principle
	// Note: Online Boutique graph is small, simple DFS is sufficient
	var longest []string

	for startNode := range dg.nodes {
		path := dg.dfsLongestPath(startNode, make(map[string]bool))
		if len(path) > len(longest) {
			longest = path
		}
	}

	return longest
}

func (dg *DependencyGraph) dfsLongestPath(u string, visited map[string]bool) []string {
	visited[u] = true
	defer delete(visited, u)

	var maxPath []string
	for _, v := range dg.nodes[u] {
		if !visited[v] {
			path := dg.dfsLongestPath(v, visited)
			if len(path) > len(maxPath) {
				maxPath = path
			}
		}
	}

	return append([]string{u}, maxPath...)
}

func (dg *DependencyGraph) loadExperimentDefaults() {
	// Explicit critical path from idea.md: frontend -> checkoutservice -> paymentservice -> currencyservice
	dg.nodes = map[string][]string{
		"frontend":        {"checkoutservice"},
		"checkoutservice": {"paymentservice", "shippingservice", "cartservice"},
		"paymentservice":  {"currencyservice"},
	}
}

// IsBuilt returns whether the graph has been constructed
func (dg *DependencyGraph) IsBuilt() bool {
	return dg.built
}

// GetGroup returns the coordination group for a given pod
func (dg *DependencyGraph) GetGroup(pod *v1.Pod) *RuntimeGroup {
	serviceName := extractServiceName(pod.Name)
	for i := range dg.groups {
		for _, svc := range dg.groups[i].Services {
			if svc == serviceName {
				return &dg.groups[i]
			}
		}
	}
	return nil
}

// IsInGroup checks if a pod belongs to any coordination group
func (dg *DependencyGraph) IsInGroup(pod *v1.Pod) bool {
	return dg.GetGroup(pod) != nil
}

// GetGroupMembers returns all service names in the same group as the pod
func (dg *DependencyGraph) GetGroupMembers(pod *v1.Pod) []string {
	group := dg.GetGroup(pod)
	if group == nil {
		return []string{}
	}
	return group.Services
}

// Clear frees all in-memory graph data.
// Called when spike window ends and gang is dissolved.
func (dg *DependencyGraph) Clear() {
	dg.nodes = make(map[string][]string)
	dg.groups = make([]RuntimeGroup, 0)
	dg.built = false
	klog.Info("Dependency graph cleared — all in-memory DAG data freed")
}

// extractServiceName extracts the service name from a pod name
// Online Boutique pods follow pattern: servicename-hash-hash
func extractServiceName(podName string) string {
	parts := strings.Split(podName, "-")
	if len(parts) >= 3 {
		// Handle multi-word service names like "redis-cart"
		// Try to match known patterns
		for i := len(parts) - 1; i >= 2; i-- {
			candidate := strings.Join(parts[:i], "-")
			if isKnownService(candidate) {
				return candidate
			}
		}
		// Default: use first part
		return parts[0]
	}
	if len(parts) > 0 {
		return parts[0]
	}
	return podName
}

// isKnownService checks if a name matches a known Online Boutique service
func isKnownService(name string) bool {
	known := map[string]bool{
		"cartservice":           true,
		"paymentservice":        true,
		"checkoutservice":       true,
		"currencyservice":       true,
		"frontend":              true,
		"productcatalogservice": true,
		"recommendationservice": true,
		"emailservice":          true,
		"shippingservice":       true,
		"adservice":             true,
		"redis-cart":            true,
		"loadgenerator":         true,
	}
	return known[name]
}
