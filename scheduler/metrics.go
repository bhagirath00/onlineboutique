/*
NEXUS Research Metrics
=====================
Provides precision latency histograms for empirically proving
low control-plane overhead. All metrics are exposed at /metrics
in Prometheus-compatible text format.

Required by Section 2F of the professional review:
"We must empirically show low control-plane overhead later."
*/

package main

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// LatencyHistogram tracks latency measurements with histogram buckets
type LatencyHistogram struct {
	mu      sync.Mutex
	name    string
	help    string
	buckets []float64 // bucket boundaries in ms
	counts  []int64   // count per bucket
	sum     float64
	count   int64
}

// NewLatencyHistogram creates a histogram with predefined buckets
func NewLatencyHistogram(name, help string) *LatencyHistogram {
	buckets := []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}
	return &LatencyHistogram{
		name:    name,
		help:    help,
		buckets: buckets,
		counts:  make([]int64, len(buckets)+1), // +1 for +Inf
	}
}

// Observe records a latency measurement in milliseconds
func (h *LatencyHistogram) Observe(ms float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sum += ms
	h.count++

	idx := sort.SearchFloat64s(h.buckets, ms)
	if idx < len(h.buckets) && h.buckets[idx] == ms {
		h.counts[idx]++
	} else if idx < len(h.buckets) {
		h.counts[idx]++
	} else {
		h.counts[len(h.buckets)]++ // +Inf bucket
	}
}

// TimeSince returns milliseconds elapsed since start and records it
func (h *LatencyHistogram) TimeSince(start time.Time) float64 {
	ms := float64(time.Since(start).Microseconds()) / 1000.0
	h.Observe(ms)
	return ms
}

// WritePrometheus writes the histogram in Prometheus text format
func (h *LatencyHistogram) WritePrometheus(w http.ResponseWriter) {
	h.mu.Lock()
	defer h.mu.Unlock()

	fmt.Fprintf(w, "# HELP %s %s\n", h.name, h.help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", h.name)

	cumulativeCount := int64(0)
	for i, boundary := range h.buckets {
		cumulativeCount += h.counts[i]
		fmt.Fprintf(w, "%s_bucket{le=\"%.0f\"} %d\n", h.name, boundary, cumulativeCount)
	}
	cumulativeCount += h.counts[len(h.buckets)]
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.name, cumulativeCount)
	fmt.Fprintf(w, "%s_sum %s\n", h.name, formatFloat(h.sum))
	fmt.Fprintf(w, "%s_count %d\n", h.name, h.count)
}

// NEXUSMetrics holds all research-grade metrics
type NEXUSMetrics struct {
	// How fast NEXUS detected the spike and transitioned to ACTIVE
	ActivationLatency *LatencyHistogram

	// How fast NEXUS built the dependency graph and formed the gang
	GangFormationLatency *LatencyHistogram

	// Overhead added to the default scheduler's Filter phase
	ExtenderFilterLatency *LatencyHistogram

	// Overhead added to the default scheduler's Prioritize phase
	ExtenderPrioritizeLatency *LatencyHistogram

	// Placement Quality Metrics (Proves Selection Intelligence)
	mu              sync.Mutex
	spikeEvents     int64
	gangsFormed     int64
	gangsDissolved  int64
	filterCalls     int64
	prioritizeCalls int64
	stateChanges    int64

	// Research-specific quality counters
	regionAPlacements  int64
	highTierPlacements int64
	apiCallsAvoided    int64 // Proves Claim 3 (Low overhead)

	currentState string
}

// NewNEXUSMetrics initializes all research metrics
func NewNEXUSMetrics() *NEXUSMetrics {
	return &NEXUSMetrics{
		ActivationLatency: NewLatencyHistogram(
			"nexus_activation_latency_ms",
			"Time from spike detection trigger to ACTIVE state (ms)",
		),
		GangFormationLatency: NewLatencyHistogram(
			"nexus_gang_formation_latency_ms",
			"Time to build dependency DAG and form temporary gang (ms)",
		),
		ExtenderFilterLatency: NewLatencyHistogram(
			"nexus_extender_filter_latency_ms",
			"Overhead added to kube-scheduler Filter phase (ms)",
		),
		ExtenderPrioritizeLatency: NewLatencyHistogram(
			"nexus_extender_prioritize_latency_ms",
			"Overhead added to kube-scheduler Prioritize phase (ms)",
		),
		currentState: "IDLE",
	}
}

// IncrementCounter safely increments a named counter
func (m *NEXUSMetrics) IncrementCounter(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch name {
	case "spike_events":
		m.spikeEvents++
	case "gangs_formed":
		m.gangsFormed++
	case "gangs_dissolved":
		m.gangsDissolved++
	case "filter_calls":
		m.filterCalls++
	case "prioritize_calls":
		m.prioritizeCalls++
	case "state_changes":
		m.stateChanges++
	case "region_a_placements":
		m.regionAPlacements++
	case "high_tier_placements":
		m.highTierPlacements++
	case "api_call_avoided":
		m.apiCallsAvoided++
	}
}

// SetState updates the current state label
func (m *NEXUSMetrics) SetState(state string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentState = state
}

// WriteAllMetrics writes all NEXUS metrics in Prometheus format
func (m *NEXUSMetrics) WriteAllMetrics(w http.ResponseWriter) {
	// Histograms
	m.ActivationLatency.WritePrometheus(w)
	m.GangFormationLatency.WritePrometheus(w)
	m.ExtenderFilterLatency.WritePrometheus(w)
	m.ExtenderPrioritizeLatency.WritePrometheus(w)

	m.mu.Lock()
	defer m.mu.Unlock()

	// State gauge
	stateValue := 0
	if m.currentState == "ACTIVE" {
		stateValue = 1
	}
	fmt.Fprintf(w, "# HELP nexus_scheduler_state Current scheduler state (0=IDLE, 1=ACTIVE)\n")
	fmt.Fprintf(w, "# TYPE nexus_scheduler_state gauge\n")
	fmt.Fprintf(w, "nexus_scheduler_state %d\n", stateValue)

	// Counters
	fmt.Fprintf(w, "# HELP nexus_spike_events_total Total spike events detected\n")
	fmt.Fprintf(w, "# TYPE nexus_spike_events_total counter\n")
	fmt.Fprintf(w, "nexus_spike_events_total %d\n", m.spikeEvents)

	fmt.Fprintf(w, "# HELP nexus_filter_calls_total Total filter extender invocations\n")
	fmt.Fprintf(w, "# TYPE nexus_filter_calls_total counter\n")
	fmt.Fprintf(w, "nexus_filter_calls_total %d\n", m.filterCalls)

	fmt.Fprintf(w, "# HELP nexus_prioritize_calls_total Total prioritize extender invocations\n")
	fmt.Fprintf(w, "# TYPE nexus_prioritize_calls_total counter\n")
	fmt.Fprintf(w, "nexus_prioritize_calls_total %d\n", m.prioritizeCalls)

	fmt.Fprintf(w, "# HELP nexus_gangs_formed_total Total gangs formed\n")
	fmt.Fprintf(w, "# TYPE nexus_gangs_formed_total counter\n")
	fmt.Fprintf(w, "nexus_gangs_formed_total %d\n", m.gangsFormed)

	fmt.Fprintf(w, "# HELP nexus_gangs_dissolved_total Total gangs dissolved\n")
	fmt.Fprintf(w, "# TYPE nexus_gangs_dissolved_total counter\n")
	fmt.Fprintf(w, "nexus_gangs_dissolved_total %d\n", m.gangsDissolved)

	// Placement Quality Counters
	fmt.Fprintf(w, "# HELP nexus_region_a_placement_total Total pods placed on Region-A nodes\n")
	fmt.Fprintf(w, "# TYPE nexus_region_a_placement_total counter\n")
	fmt.Fprintf(w, "nexus_region_a_placement_total %d\n", m.regionAPlacements)

	fmt.Fprintf(w, "# HELP nexus_high_tier_placement_total Total pods placed on high-performance nodes\n")
	fmt.Fprintf(w, "# TYPE nexus_high_tier_placement_total counter\n")
	fmt.Fprintf(w, "nexus_high_tier_placement_total %d\n", m.highTierPlacements)

	fmt.Fprintf(w, "# HELP nexus_api_calls_avoided_total Total API calls avoided by in-memory caching\n")
	fmt.Fprintf(w, "# TYPE nexus_api_calls_avoided_total counter\n")
	fmt.Fprintf(w, "nexus_api_calls_avoided_total %d\n", m.apiCallsAvoided)

	fmt.Fprintf(w, "# HELP nexus_state_changes_total Total IDLE/ACTIVE state transitions\n")
	fmt.Fprintf(w, "# TYPE nexus_state_changes_total counter\n")
	fmt.Fprintf(w, "nexus_state_changes_total %d\n", m.stateChanges)
}

// formatFloat formats a float for Prometheus output
func formatFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	return fmt.Sprintf("%.3f", f)
}
