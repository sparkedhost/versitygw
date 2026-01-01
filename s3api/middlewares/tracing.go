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

package middlewares

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// TracingMiddleware adds OpenTelemetry tracing to all requests
func TracingMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Skip health check endpoints
		path := c.Path()
		if path == "/health" || path == "/ready" || path == "/metrics" {
			return c.Next()
		}

		// Extract trace context from incoming headers
		ctx := c.UserContext()
		carrier := make(propagation.HeaderCarrier)
		c.Request().Header.VisitAll(func(key, value []byte) {
			carrier.Set(string(key), string(value))
		})
		ctx = propagation.TraceContext{}.Extract(ctx, carrier)

		// Determine the operation name
		operationName := determineOperationName(c)

		// Start the span
		ctx, span := observability.Tracer().Start(ctx, operationName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPMethod(c.Method()),
				semconv.HTTPURL(c.OriginalURL()),
				semconv.HTTPScheme(c.Protocol()),
				semconv.NetHostName(c.Hostname()),
				attribute.String("http.client_ip", c.IP()),
				attribute.String("http.user_agent", c.Get("User-Agent")),
				attribute.String("http.request_id", c.Get("X-Request-ID")),
			),
		)
		defer span.End()

		// Add S3-specific attributes if available
		bucket := c.Params("bucket")
		key := c.Params("key")
		if bucket != "" {
			span.SetAttributes(attribute.String("s3.bucket", bucket))
		}
		if key != "" {
			span.SetAttributes(attribute.String("s3.key", key))
		}

		// Store trace context for use in handlers
		c.SetUserContext(ctx)

		// Add trace ID to response headers for debugging
		if span.SpanContext().HasTraceID() {
			c.Set("X-Trace-ID", span.SpanContext().TraceID().String())
		}

		// Execute the request
		err := c.Next()

		// Record response status
		statusCode := c.Response().StatusCode()
		span.SetAttributes(attribute.Int("http.status_code", statusCode))

		// Record content length if available
		if contentLength := c.Response().Header.ContentLength(); contentLength > 0 {
			span.SetAttributes(attribute.Int64("http.response_content_length", int64(contentLength)))
		}

		// Set span status based on HTTP status code
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else if statusCode >= 500 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
		} else if statusCode >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("Client error: HTTP %d", statusCode))
		} else {
			span.SetStatus(codes.Ok, "")
		}

		return err
	}
}

// determineOperationName extracts the S3 operation name from the request
func determineOperationName(c *fiber.Ctx) string {
	method := c.Method()
	bucket := c.Params("bucket")
	key := c.Params("key")

	// Check for specific query parameters that indicate the operation
	if c.Query("acl") != "" {
		if method == "GET" {
			return "S3.GetObjectAcl"
		} else if method == "PUT" {
			return "S3.PutObjectAcl"
		}
	}
	if c.Query("uploads") != "" {
		if method == "GET" {
			return "S3.ListMultipartUploads"
		} else if method == "POST" && key != "" {
			return "S3.CreateMultipartUpload"
		}
	}
	if c.Query("uploadId") != "" {
		if method == "GET" {
			return "S3.ListParts"
		} else if method == "PUT" {
			return "S3.UploadPart"
		} else if method == "POST" {
			return "S3.CompleteMultipartUpload"
		} else if method == "DELETE" {
			return "S3.AbortMultipartUpload"
		}
	}
	if c.Query("tagging") != "" {
		if method == "GET" {
			return "S3.GetObjectTagging"
		} else if method == "PUT" {
			return "S3.PutObjectTagging"
		} else if method == "DELETE" {
			return "S3.DeleteObjectTagging"
		}
	}
	if c.Query("versioning") != "" {
		if method == "GET" {
			return "S3.GetBucketVersioning"
		} else if method == "PUT" {
			return "S3.PutBucketVersioning"
		}
	}
	if c.Query("versions") != "" {
		return "S3.ListObjectVersions"
	}
	if c.Query("delete") != "" {
		return "S3.DeleteObjects"
	}
	if c.Query("cors") != "" {
		if method == "GET" {
			return "S3.GetBucketCors"
		} else if method == "PUT" {
			return "S3.PutBucketCors"
		} else if method == "DELETE" {
			return "S3.DeleteBucketCors"
		}
	}
	if c.Query("lifecycle") != "" {
		if method == "GET" {
			return "S3.GetBucketLifecycle"
		} else if method == "PUT" {
			return "S3.PutBucketLifecycle"
		}
	}
	if c.Query("policy") != "" {
		if method == "GET" {
			return "S3.GetBucketPolicy"
		} else if method == "PUT" {
			return "S3.PutBucketPolicy"
		} else if method == "DELETE" {
			return "S3.DeleteBucketPolicy"
		}
	}
	if c.Query("location") != "" {
		return "S3.GetBucketLocation"
	}

	// Basic operations based on method and path structure
	switch method {
	case "GET":
		if bucket == "" {
			return "S3.ListBuckets"
		} else if key == "" {
			return "S3.ListObjects"
		}
		return "S3.GetObject"
	case "PUT":
		if bucket != "" && key == "" {
			return "S3.CreateBucket"
		}
		return "S3.PutObject"
	case "DELETE":
		if key == "" {
			return "S3.DeleteBucket"
		}
		return "S3.DeleteObject"
	case "HEAD":
		if key == "" {
			return "S3.HeadBucket"
		}
		return "S3.HeadObject"
	case "POST":
		if c.Query("delete") != "" {
			return "S3.DeleteObjects"
		}
		return "S3.PostObject"
	case "OPTIONS":
		return "S3.Options"
	}

	return fmt.Sprintf("S3.%s", method)
}

// TracingConfig holds configuration for the tracing middleware
type TracingConfig struct {
	// SkipPaths are paths that should not be traced
	SkipPaths []string
	// IncludeHeaders determines if request headers should be included
	IncludeHeaders bool
	// IncludeBody determines if request body should be included (be careful with large payloads)
	IncludeBody bool
}

// TracingMiddlewareWithConfig creates a tracing middleware with custom configuration
func TracingMiddlewareWithConfig(cfg TracingConfig) fiber.Handler {
	skipMap := make(map[string]bool)
	for _, path := range cfg.SkipPaths {
		skipMap[path] = true
	}

	return func(c *fiber.Ctx) error {
		path := c.Path()

		// Skip configured paths
		if skipMap[path] {
			return c.Next()
		}

		// Extract trace context from incoming headers
		ctx := c.UserContext()
		carrier := make(propagation.HeaderCarrier)
		c.Request().Header.VisitAll(func(key, value []byte) {
			carrier.Set(string(key), string(value))
		})
		ctx = propagation.TraceContext{}.Extract(ctx, carrier)

		operationName := determineOperationName(c)

		attrs := []attribute.KeyValue{
			semconv.HTTPMethod(c.Method()),
			semconv.HTTPURL(c.OriginalURL()),
			semconv.HTTPScheme(c.Protocol()),
			semconv.NetHostName(c.Hostname()),
			attribute.String("http.client_ip", c.IP()),
		}

		// Optionally include headers
		if cfg.IncludeHeaders {
			c.Request().Header.VisitAll(func(key, value []byte) {
				k := string(key)
				// Skip sensitive headers
				if k != "Authorization" && k != "X-Amz-Security-Token" {
					attrs = append(attrs, attribute.String("http.request.header."+k, string(value)))
				}
			})
		}

		ctx, span := observability.Tracer().Start(ctx, operationName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attrs...),
		)
		defer span.End()

		// Add S3-specific attributes
		if bucket := c.Params("bucket"); bucket != "" {
			span.SetAttributes(attribute.String("s3.bucket", bucket))
		}
		if key := c.Params("key"); key != "" {
			span.SetAttributes(attribute.String("s3.key", key))
		}

		c.SetUserContext(ctx)

		if span.SpanContext().HasTraceID() {
			c.Set("X-Trace-ID", span.SpanContext().TraceID().String())
		}

		err := c.Next()

		statusCode := c.Response().StatusCode()
		span.SetAttributes(attribute.Int("http.status_code", statusCode))

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else if statusCode >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
		} else {
			span.SetStatus(codes.Ok, "")
		}

		return err
	}
}
