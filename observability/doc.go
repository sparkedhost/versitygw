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

// Package observability provides unified observability features including
// tracing (OpenTelemetry), error reporting (Sentry), and metrics (Prometheus).
//
// # Quick Start
//
// Initialize all observability features at startup:
//
//	func main() {
//	    ctx := context.Background()
//
//	    // Initialize OpenTelemetry
//	    err := observability.InitOTEL(ctx, observability.OTELConfig{
//	        Enabled:        true,
//	        Endpoint:       "localhost:4317",
//	        ServiceName:    "versitygw",
//	        ServiceVersion: "1.0.0",
//	        Environment:    "production",
//	        SampleRate:     1.0,
//	    })
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    defer observability.ShutdownOTEL(ctx)
//
//	    // Initialize Sentry
//	    err = observability.InitSentry(observability.SentryConfig{
//	        Enabled:          true,
//	        DSN:              "https://xxx@sentry.io/xxx",
//	        Environment:      "production",
//	        Release:          "1.0.0",
//	        SampleRate:       1.0,
//	        TracesSampleRate: 0.1,
//	    })
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    defer observability.ShutdownSentry(2 * time.Second)
//
//	    // Initialize Prometheus
//	    err = observability.InitPrometheus(observability.PrometheusConfig{
//	        Enabled:   true,
//	        Port:      "9090",
//	        Path:      "/metrics",
//	        Namespace: "versitygw",
//	    })
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    defer observability.ShutdownPrometheus(ctx)
//	}
//
// # Tracing
//
// Use tracing to track request flow:
//
//	func HandleUploadPart(ctx context.Context, bucket, key string) error {
//	    ctx, span := observability.StartSpan(ctx, "UploadPart",
//	        trace.WithAttributes(observability.S3Attributes(bucket, key, "UploadPart")...))
//	    defer span.End()
//
//	    // Your code here...
//	    if err != nil {
//	        observability.SetError(ctx, err)
//	        return err
//	    }
//
//	    return nil
//	}
//
// # Error Reporting
//
// Report errors with context:
//
//	if err != nil {
//	    observability.ReportError(ctx, err, map[string]interface{}{
//	        "bucket":    bucket,
//	        "key":       key,
//	        "operation": "UploadPart",
//	    })
//	}
//
// For multipart-specific errors:
//
//	mpErr := observability.NewMultipartError(err, "open_part", bucket, key, uploadID, partNum, partPath)
//	observability.ReportMultipartError(ctx, mpErr)
//
// # Metrics
//
// Record metrics for monitoring:
//
//	observability.RecordS3Operation("PutObject", bucket, "success", duration)
//	observability.RecordMultipartPart(bucket, partSize)
//	observability.RecordMultipartError(bucket, "not_found")
package observability
