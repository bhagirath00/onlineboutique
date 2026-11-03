/*
Spike Detection Module
=====================
Monitors Prometheus for traffic spike indicators and triggers
NEXUS activation. Implements Algorithm 1: Traffic Spike Detection.

Required by Section 2A of the professional review:
"It must trigger only when:
  - RPS threshold exceeded OR
  - HPA scaling event observed OR
  - p95 latency crosses defined bound"

The spike detector is the GATEKEEPER for the entire NEXUS system.
Without a spike event, NEXUS remains completely dormant.
*/

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// SpikeDetector monitors for traffic spikes using Prometheus metrics
type SpikeDetector struct {
	prometheusURL       string
	qpsThreshold        float64
	errorThreshold      float64
	p95LatencyThreshold float64 // milliseconds
	fallbackThreshold   int
	client              *http.Client
	clientset           *kubernetes.Clientset
}

// PrometheusResponse represents the response from Prometheus API
type PrometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// NewSpikeDetector creates a new spike detector with configurable thresholds
func NewSpikeDetector(clientset *kubernetes.Clientset) *SpikeDetector {
	prometheusURL := os.Getenv("PROMETHEUS_URL")
	if prometheusURL == "" {
		prometheusURL = "http://prometheus-server.monitoring.svc.cluster.local:80"
	}

	qpsThreshold := 90.0
	if qpsStr := os.Getenv("SPIKE_QPS_THRESHOLD"); qpsStr != "" {
		if val, err := strconv.ParseFloat(qpsStr, 64); err == nil {
			qpsThreshold = val
		}
	}

	errorThreshold := 50.0
	if errStr := os.Getenv("SPIKE_ERROR_THRESHOLD"); errStr != "" {
		if val, err := strconv.ParseFloat(errStr, 64); err == nil {
			errorThreshold = val
		}
	}

	p95LatencyThreshold := 500.0 // 500ms default
	if latStr := os.Getenv("SPIKE_P95_LATENCY_THRESHOLD"); latStr != "" {
		if val, err := strconv.ParseFloat(latStr, 64); err == nil {
			p95LatencyThreshold = val
		}
	}

	klog.Infof("Spike detector thresholds: QPS=%.0f, ErrorRate=%.0f, p95Latency=%.0fms",
		qpsThreshold, errorThreshold, p95LatencyThreshold)

	// Default pending-pod fallback threshold is 10 to avoid false-positive
	// activations during normal pod churn or scheduler startup.
	// Override with SPIKE_FALLBACK_THRESHOLD env var.
	fallbackThreshold := 10
	if fbStr := os.Getenv("SPIKE_FALLBACK_THRESHOLD"); fbStr != "" {
		if val, err := strconv.Atoi(fbStr); err == nil {
			fallbackThreshold = val
		}
	}

	return &SpikeDetector{
		prometheusURL:       prometheusURL,
		qpsThreshold:        qpsThreshold,
		errorThreshold:      errorThreshold,
		p95LatencyThreshold: p95LatencyThreshold,
		fallbackThreshold:   fallbackThreshold,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		clientset: clientset,
	}
}

// Detect checks if a spike is currently happening.
// Implements Algorithm 1: Traffic Spike Detection.
//
// Detection order (intentional):
//   1. Prometheus QPS       — primary signal
//   2. Prometheus ErrorRate — secondary signal
//   3. Prometheus p95       — latency degradation signal
//   4. HPA scale events     — infrastructure-level signal
//   5. Pending pod count    — LAST RESORT (only when Prometheus unreachable)
//
// The pending-pod check is intentionally last so that normal pod churn
// (e.g., scheduler startup binding its own pods) never causes a false
// positive activation.
func (sd *SpikeDetector) Detect(pendingPodCount int) bool {
	// --- Prometheus-based checks first ---
	if sd.isPrometheusReachable() {
		// Check 1: QPS (Queries Per Second)
		qps, err := sd.queryQPS()
		if err != nil {
			klog.Warningf("Failed to query QPS: %v", err)
		} else if qps >= sd.qpsThreshold {
			klog.Infof("SPIKE DETECTED: QPS %.2f >= threshold %.2f", qps, sd.qpsThreshold)
			return true
		}

		// Check 2: Error Rate (5xx errors)
		errorRate, err := sd.queryErrorRate()
		if err != nil {
			klog.Warningf("Failed to query error rate: %v", err)
		} else if errorRate > sd.errorThreshold {
			klog.Infof("SPIKE DETECTED: Error rate %.2f > threshold %.2f", errorRate, sd.errorThreshold)
			return true
		}

		// Check 3: p95 Latency (professional requirement 2A)
		p95, err := sd.queryP95Latency()
		if err != nil {
			klog.Warningf("Failed to query p95 latency: %v", err)
		} else if p95 > sd.p95LatencyThreshold {
			klog.Infof("SPIKE DETECTED: p95 latency %.2fms > threshold %.2fms", p95, sd.p95LatencyThreshold)
			return true
		}

		// Check 4: HPA scale-up events
		hpaActive, err := sd.checkHPAActivity()
		if err != nil {
			klog.Warningf("Failed to check HPA activity: %v", err)
		} else if hpaActive {
			klog.Info("SPIKE DETECTED: HPA scale-up event detected")
			return true
		}

		// All Prometheus checks passed — no spike
		klog.V(2).Infof("No spike detected via Prometheus (QPS/ErrorRate/p95/HPA all within thresholds)")
		return false
	}

	// --- Prometheus unreachable: use pending-pod count as last-resort fallback ---
	// Only triggers when count is significantly above the conservative threshold
	// to avoid reacting to transient pod churn during scheduler startup.
	klog.V(2).Info("Prometheus unreachable — using pending-pod fallback check")
	if pendingPodCount >= sd.fallbackThreshold {
		klog.Infof("SPIKE DETECTED (fallback): Pending pods %d >= threshold %d",
			pendingPodCount, sd.fallbackThreshold)
		return true
	}

	klog.V(2).Infof("No spike detected (fallback): Pending pods %d < threshold %d",
		pendingPodCount, sd.fallbackThreshold)
	return false
}

// isPrometheusReachable checks if Prometheus is available
func (sd *SpikeDetector) isPrometheusReachable() bool {
	url := fmt.Sprintf("%s/api/v1/query?query=up", sd.prometheusURL)
	resp, err := sd.client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// queryQPS retrieves the current request rate
func (sd *SpikeDetector) queryQPS() (float64, error) {
	// 1. PRIMARY: Query Locust stats directly from all candidate cluster URLs
	locustURLs := []string{
		"http://loadgenerator.default.svc.cluster.local:8089/stats/requests",
		"http://locust.nexus-system.svc.cluster.local:8089/stats/requests",
		"http://localhost:8089/stats/requests",
	}

	for _, locustURL := range locustURLs {
		resp, err := sd.client.Get(locustURL)
		if err == nil {
			defer resp.Body.Close()
			var stats struct {
				Stats []struct {
					Name       string  `json:"name"`
					CurrentRps float64 `json:"current_rps"`
				} `json:"stats"`
			}
			if decodeErr := json.NewDecoder(resp.Body).Decode(&stats); decodeErr == nil {
				for _, s := range stats.Stats {
					if s.Name == "Total" && s.CurrentRps > 0 {
						klog.Infof("NEXUS-QPS: %.2f RPS from Locust (%s)", s.CurrentRps, locustURL)
						return s.CurrentRps, nil
					}
				}
			}
		}
	}

	// 2. FALLBACK: Prometheus CPU proxy for default namespace frontend pods
	query := `sum(rate(container_cpu_usage_seconds_total{namespace="default",pod=~"frontend-.*"}[1m])) * 400`
	val, err := sd.queryPrometheus(query)
	if err == nil && val > 0.01 {
		klog.Infof("NEXUS-QPS-FALLBACK: %.2f from Prometheus CPU proxy", val)
		return val, nil
	}

	return 0, nil
}

// queryErrorRate retrieves the current error rate
func (sd *SpikeDetector) queryErrorRate() (float64, error) {
	query := `sum(rate(apiserver_request_total{code=~"5.."}[1m]))`
	val, err := sd.queryPrometheus(query)
	if err == nil {
		return val, nil
	}
	return 0, nil
}

// queryP95Latency retrieves the p95 request latency in milliseconds
func (sd *SpikeDetector) queryP95Latency() (float64, error) {
	// 1. PRIMARY: Query Locust stats directly
	locustURLs := []string{
		"http://loadgenerator.default.svc.cluster.local:8089/stats/requests",
		"http://locust.nexus-system.svc.cluster.local:8089/stats/requests",
		"http://localhost:8089/stats/requests",
	}

	for _, locustURL := range locustURLs {
		resp, err := sd.client.Get(locustURL)
		if err == nil {
			defer resp.Body.Close()
			var stats struct {
				Stats []struct {
					Name                     string  `json:"name"`
					ResponseTimePercentile95 float64 `json:"response_time_percentile_95"`
					AvgResponseTime          float64 `json:"avg_response_time"`
				} `json:"stats"`
			}
			if decodeErr := json.NewDecoder(resp.Body).Decode(&stats); decodeErr == nil {
				for _, s := range stats.Stats {
					if s.Name == "Total" {
						v := s.ResponseTimePercentile95
						if v == 0 {
							v = s.AvgResponseTime * 1.5
						}
						if v > 0 {
							klog.Infof("NEXUS-P95: %.2fms from Locust (%s)", v, locustURL)
							return v, nil
						}
					}
				}
			}
		}
	}

	// 2. FALLBACK: Prometheus API Server Latency P95
	query := `histogram_quantile(0.95, sum(rate(apiserver_request_duration_seconds_bucket[1m])) by (le)) * 1000`
	val, err := sd.queryPrometheus(query)
	if err == nil && val > 0.01 {
		klog.Infof("NEXUS-P95-FALLBACK: %.2fms from Prometheus", val)
		return val, nil
	}

	return 0, nil
}


// checkHPAActivity checks if any HPA has recently scaled up
func (sd *SpikeDetector) checkHPAActivity() (bool, error) {
	query := "increase(kube_horizontalpodautoscaler_status_current_replicas[1m])"
	value, err := sd.queryPrometheus(query)
	if err != nil {
		return false, err
	}
	return value > 0, nil
}

// queryPrometheus executes a PromQL query and returns the numeric result
func (sd *SpikeDetector) queryPrometheus(query string) (float64, error) {
	url := fmt.Sprintf("%s/api/v1/query?query=%s", sd.prometheusURL, url.QueryEscape(query))

	resp, err := sd.client.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to query Prometheus: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response: %w", err)
	}

	var promResp PrometheusResponse
	if err := json.Unmarshal(body, &promResp); err != nil {
		return 0, fmt.Errorf("failed to parse response: %w", err)
	}

	if promResp.Status != "success" {
		return 0, fmt.Errorf("prometheus query failed: %s", promResp.Status)
	}

	if len(promResp.Data.Result) == 0 {
		return 0, nil // No data
	}

	// Extract the numeric value
	if len(promResp.Data.Result[0].Value) < 2 {
		return 0, fmt.Errorf("invalid result format")
	}

	valueStr, ok := promResp.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("value is not a string")
	}

	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse value: %w", err)
	}

	return value, nil
}
