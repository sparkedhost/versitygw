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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// SpanKind aliases for convenience
const (
	SpanKindServer   = trace.SpanKindServer
	SpanKindClient   = trace.SpanKindClient
	SpanKindInternal = trace.SpanKindInternal
)

// Common attribute keys for S3 operations
const (
	AttrS3Bucket        = "s3.bucket"
	AttrS3Key           = "s3.key"
	AttrS3Operation     = "s3.operation"
	AttrS3UploadID      = "s3.upload_id"
	AttrS3PartNumber    = "s3.part_number"
	AttrS3VersionID     = "s3.version_id"
	AttrS3ContentLength = "s3.content_length"
	AttrS3ETag          = "s3.etag"
	AttrS3StorageClass  = "s3.storage_class"

	AttrBackendType      = "backend.type"
	AttrBackendOperation = "backend.operation"
	AttrBackendPath      = "backend.path"

	AttrAuthMethod   = "auth.method"
	AttrAuthAccount  = "auth.account"
	AttrAuthSuccess  = "auth.success"
	AttrAuthError    = "auth.error"

	AttrHTTPMethod     = "http.method"
	AttrHTTPURL        = "http.url"
	AttrHTTPStatusCode = "http.status_code"
	AttrHTTPClientIP   = "http.client_ip"
	AttrHTTPUserAgent  = "http.user_agent"
)

// TracingContext wraps a context with tracing utilities
type TracingContext struct {
	ctx  context.Context
	span trace.Span
}

// NewTracingContext creates a new tracing context
func NewTracingContext(ctx context.Context, span trace.Span) *TracingContext {
	return &TracingContext{ctx: ctx, span: span}
}

// Context returns the underlying context
func (tc *TracingContext) Context() context.Context {
	return tc.ctx
}

// Span returns the span
func (tc *TracingContext) Span() trace.Span {
	return tc.span
}

// End ends the span
func (tc *TracingContext) End() {
	tc.span.End()
}

// SetStatus sets the span status
func (tc *TracingContext) SetStatus(code codes.Code, description string) {
	tc.span.SetStatus(code, description)
}

// SetOK sets the span status to OK
func (tc *TracingContext) SetOK() {
	tc.span.SetStatus(codes.Ok, "")
}

// SetError records an error and sets error status
func (tc *TracingContext) SetError(err error) {
	if err != nil {
		tc.span.RecordError(err)
		tc.span.SetStatus(codes.Error, err.Error())
	}
}

// AddEvent adds an event with optional attributes
func (tc *TracingContext) AddEvent(name string, attrs ...attribute.KeyValue) {
	tc.span.AddEvent(name, trace.WithAttributes(attrs...))
}

// SetAttributes sets attributes on the span
func (tc *TracingContext) SetAttributes(attrs ...attribute.KeyValue) {
	tc.span.SetAttributes(attrs...)
}

// StartHTTPSpan starts a span for an HTTP request
func StartHTTPSpan(ctx context.Context, r *http.Request, operationName string) (context.Context, trace.Span) {
	// Extract trace context from incoming request headers
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.HeaderCarrier(r.Header))

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			semconv.HTTPMethod(r.Method),
			semconv.HTTPURL(r.URL.String()),
			semconv.HTTPScheme(r.URL.Scheme),
			semconv.NetHostName(r.Host),
			attribute.String(AttrHTTPClientIP, r.RemoteAddr),
			attribute.String(AttrHTTPUserAgent, r.UserAgent()),
		),
	}

	return Tracer().Start(ctx, operationName, opts...)
}

// StartS3OperationSpan starts a span for an S3 operation
func StartS3OperationSpan(ctx context.Context, operation, bucket, key string) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrS3Operation, operation),
	}
	if bucket != "" {
		attrs = append(attrs, attribute.String(AttrS3Bucket, bucket))
	}
	if key != "" {
		attrs = append(attrs, attribute.String(AttrS3Key, key))
	}

	return Tracer().Start(ctx, fmt.Sprintf("S3.%s", operation),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)
}

// StartBackendSpan starts a span for a backend operation
func StartBackendSpan(ctx context.Context, backendType, operation string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, fmt.Sprintf("Backend.%s.%s", backendType, operation),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String(AttrBackendType, backendType),
			attribute.String(AttrBackendOperation, operation),
		),
	)
}

// StartAuthSpan starts a span for authentication
func StartAuthSpan(ctx context.Context, method string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, fmt.Sprintf("Auth.%s", method),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String(AttrAuthMethod, method),
		),
	)
}

// StartMultipartSpan starts a span for multipart operations
func StartMultipartSpan(ctx context.Context, operation, bucket, key, uploadID string, partNumber int32) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrS3Operation, operation),
		attribute.String(AttrS3Bucket, bucket),
		attribute.String(AttrS3Key, key),
	}
	if uploadID != "" {
		attrs = append(attrs, attribute.String(AttrS3UploadID, uploadID))
	}
	if partNumber > 0 {
		attrs = append(attrs, attribute.Int(AttrS3PartNumber, int(partNumber)))
	}

	return Tracer().Start(ctx, fmt.Sprintf("S3.Multipart.%s", operation),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)
}

// StartInternalSpan starts an internal span for subroutines
func StartInternalSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrs...),
	)
}

// EndSpanWithError ends a span and records any error
func EndSpanWithError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// EndSpanWithStatus ends a span with an HTTP status code
func EndSpanWithStatus(span trace.Span, statusCode int, err error) {
	span.SetAttributes(attribute.Int(AttrHTTPStatusCode, statusCode))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else if statusCode >= 400 {
		span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

// RecordDuration records the duration of an operation
func RecordDuration(span trace.Span, start time.Time) {
	duration := time.Since(start)
	span.SetAttributes(
		attribute.Int64("duration_ms", duration.Milliseconds()),
		attribute.Float64("duration_seconds", duration.Seconds()),
	)
}

// WithTracedOperation wraps an operation with tracing
func WithTracedOperation(ctx context.Context, name string, attrs []attribute.KeyValue, fn func(ctx context.Context) error) error {
	ctx, span := StartInternalSpan(ctx, name, attrs...)
	defer span.End()

	start := time.Now()
	err := fn(ctx)
	RecordDuration(span, start)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// WithTracedOperationResult wraps an operation with result and tracing
func WithTracedOperationResult[T any](ctx context.Context, name string, attrs []attribute.KeyValue, fn func(ctx context.Context) (T, error)) (T, error) {
	ctx, span := StartInternalSpan(ctx, name, attrs...)
	defer span.End()

	start := time.Now()
	result, err := fn(ctx)
	RecordDuration(span, start)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return result, err
}

// InjectTraceContext injects trace context into outgoing HTTP headers
func InjectTraceContext(ctx context.Context, headers http.Header) {
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(headers))
}

// ExtractTraceContext extracts trace context from incoming HTTP headers
func ExtractTraceContext(ctx context.Context, headers http.Header) context.Context {
	return propagation.TraceContext{}.Extract(ctx, propagation.HeaderCarrier(headers))
}

// TraceID returns the trace ID from the context
func TraceID(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().HasTraceID() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}

// SpanID returns the span ID from the context
func SpanID(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().HasSpanID() {
		return span.SpanContext().SpanID().String()
	}
	return ""
}

// IsTracingEnabled checks if the current context has an active trace
func IsTracingEnabled(ctx context.Context) bool {
	span := trace.SpanFromContext(ctx)
	return span.SpanContext().IsValid()
}

// LogFields returns trace fields for logging
func LogFields(ctx context.Context) map[string]string {
	fields := make(map[string]string)
	if traceID := TraceID(ctx); traceID != "" {
		fields["trace_id"] = traceID
	}
	if spanID := SpanID(ctx); spanID != "" {
		fields["span_id"] = spanID
	}
	return fields
}
