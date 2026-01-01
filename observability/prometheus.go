// Copyright 2024 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package observability

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusConfig holds Prometheus metrics configuration
type PrometheusConfig struct {
	// Enabled enables Prometheus metrics
	Enabled bool
	// Path is the HTTP path for metrics endpoint (default: /metrics)
	Path string
	// Port is the port to expose metrics on
	Port string
	// Namespace is the metrics namespace prefix
	Namespace string
}

var (
	// Request metrics
	requestsTotal *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	requestSize *prometheus.HistogramVec
	responseSize *prometheus.HistogramVec

	// S3 operation metrics
	s3OperationsTotal *prometheus.CounterVec
	s3OperationDuration *prometheus.HistogramVec
	s3OperationErrors *prometheus.CounterVec

	// Multipart upload metrics
	multipartUploadsActive *prometheus.GaugeVec
	multipartPartsTotal *prometheus.CounterVec
	multipartPartSize *prometheus.HistogramVec
	multipartErrors *prometheus.CounterVec

	// Backend metrics
	backendOperationsTotal *prometheus.CounterVec
	backendOperationDuration *prometheus.HistogramVec
	backendErrors *prometheus.CounterVec

	// System metrics
	activeConnections prometheus.Gauge
	goroutines prometheus.Gauge

	metricsServer *http.Server
	prometheusEnabled bool
)

// InitPrometheus initializes Prometheus metrics
func InitPrometheus(cfg PrometheusConfig) error {
	if !cfg.Enabled {
		prometheusEnabled = false
		return nil
	}

	namespace := cfg.Namespace
	if namespace == "" {
		namespace = "versitygw"
	}

	// Request metrics
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	requestSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_size_bytes",
			Help:      "HTTP request size in bytes",
			Buckets:   prometheus.ExponentialBuckets(100, 10, 8),
		},
		[]string{"method", "path"},
	)

	responseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_response_size_bytes",
			Help:      "HTTP response size in bytes",
			Buckets:   prometheus.ExponentialBuckets(100, 10, 8),
		},
		[]string{"method", "path"},
	)

	// S3 operation metrics
	s3OperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "s3_operations_total",
			Help:      "Total number of S3 operations",
		},
		[]string{"operation", "bucket", "status"},
	)

	s3OperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "s3_operation_duration_seconds",
			Help:      "S3 operation duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation", "bucket"},
	)

	s3OperationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "s3_operation_errors_total",
			Help:      "Total number of S3 operation errors",
		},
		[]string{"operation", "bucket", "error_code"},
	)

	// Multipart upload metrics
	multipartUploadsActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "multipart_uploads_active",
			Help:      "Number of active multipart uploads",
		},
		[]string{"bucket"},
	)

	multipartPartsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "multipart_parts_total",
			Help:      "Total number of multipart parts uploaded",
		},
		[]string{"bucket"},
	)

	multipartPartSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "multipart_part_size_bytes",
			Help:      "Size of multipart parts in bytes",
			Buckets:   prometheus.ExponentialBuckets(1024*1024, 2, 12), // 1MB to 4GB
		},
		[]string{"bucket"},
	)

	multipartErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "multipart_errors_total",
			Help:      "Total number of multipart upload errors",
		},
		[]string{"bucket", "error_type"},
	)

	// Backend metrics
	backendOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "backend_operations_total",
			Help:      "Total number of backend operations",
		},
		[]string{"backend", "operation", "status"},
	)

	backendOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "backend_operation_duration_seconds",
			Help:      "Backend operation duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"backend", "operation"},
	)

	backendErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "backend_errors_total",
			Help:      "Total number of backend errors",
		},
		[]string{"backend", "operation", "error_type"},
	)

	// System metrics
	activeConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_connections",
			Help:      "Number of active connections",
		},
	)

	goroutines = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "goroutines",
			Help:      "Number of goroutines",
		},
	)

	// Register all metrics
	prometheus.MustRegister(
		requestsTotal,
		requestDuration,
		requestSize,
		responseSize,
		s3OperationsTotal,
		s3OperationDuration,
		s3OperationErrors,
		multipartUploadsActive,
		multipartPartsTotal,
		multipartPartSize,
		multipartErrors,
		backendOperationsTotal,
		backendOperationDuration,
		backendErrors,
		activeConnections,
		goroutines,
	)

	// Start metrics server
	path := cfg.Path
	if path == "" {
		path = "/metrics"
	}
	port := cfg.Port
	if port == "" {
		port = "9090"
	}

	mux := http.NewServeMux()
	mux.Handle(path, promhttp.Handler())

	metricsServer = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Prometheus metrics server error: %v\n", err)
		}
	}()

	prometheusEnabled = true
	return nil
}

// ShutdownPrometheus gracefully shuts down the Prometheus metrics server
func ShutdownPrometheus(ctx context.Context) error {
	if metricsServer != nil {
		return metricsServer.Shutdown(ctx)
	}
	return nil
}

// IsPrometheusEnabled returns whether Prometheus metrics are enabled
func IsPrometheusEnabled() bool {
	return prometheusEnabled
}

// RecordRequest records an HTTP request metric
func RecordRequest(method, path string, status int, duration time.Duration, reqSize, respSize int64) {
	if !prometheusEnabled {
		return
	}

	statusStr := fmt.Sprintf("%d", status)
	requestsTotal.WithLabelValues(method, path, statusStr).Inc()
	requestDuration.WithLabelValues(method, path).Observe(duration.Seconds())
	requestSize.WithLabelValues(method, path).Observe(float64(reqSize))
	responseSize.WithLabelValues(method, path).Observe(float64(respSize))
}

// RecordS3Operation records an S3 operation metric
func RecordS3Operation(operation, bucket, status string, duration time.Duration) {
	if !prometheusEnabled {
		return
	}

	s3OperationsTotal.WithLabelValues(operation, bucket, status).Inc()
	s3OperationDuration.WithLabelValues(operation, bucket).Observe(duration.Seconds())
}

// RecordS3Error records an S3 operation error
func RecordS3Error(operation, bucket, errorCode string) {
	if !prometheusEnabled {
		return
	}

	s3OperationErrors.WithLabelValues(operation, bucket, errorCode).Inc()
}

// RecordMultipartUploadStart records the start of a multipart upload
func RecordMultipartUploadStart(bucket string) {
	if !prometheusEnabled {
		return
	}

	multipartUploadsActive.WithLabelValues(bucket).Inc()
}

// RecordMultipartUploadComplete records the completion of a multipart upload
func RecordMultipartUploadComplete(bucket string) {
	if !prometheusEnabled {
		return
	}

	multipartUploadsActive.WithLabelValues(bucket).Dec()
}

// RecordMultipartPart records a multipart part upload
func RecordMultipartPart(bucket string, size int64) {
	if !prometheusEnabled {
		return
	}

	multipartPartsTotal.WithLabelValues(bucket).Inc()
	multipartPartSize.WithLabelValues(bucket).Observe(float64(size))
}

// RecordMultipartError records a multipart upload error
func RecordMultipartError(bucket, errorType string) {
	if !prometheusEnabled {
		return
	}

	multipartErrors.WithLabelValues(bucket, errorType).Inc()
}

// RecordBackendOperation records a backend operation metric
func RecordBackendOperation(backend, operation, status string, duration time.Duration) {
	if !prometheusEnabled {
		return
	}

	backendOperationsTotal.WithLabelValues(backend, operation, status).Inc()
	backendOperationDuration.WithLabelValues(backend, operation).Observe(duration.Seconds())
}

// RecordBackendError records a backend error
func RecordBackendError(backend, operation, errorType string) {
	if !prometheusEnabled {
		return
	}

	backendErrors.WithLabelValues(backend, operation, errorType).Inc()
}

// SetActiveConnections sets the number of active connections
func SetActiveConnections(count int) {
	if !prometheusEnabled {
		return
	}

	activeConnections.Set(float64(count))
}

// SetGoroutines sets the number of goroutines
func SetGoroutines(count int) {
	if !prometheusEnabled {
		return
	}

	goroutines.Set(float64(count))
}
