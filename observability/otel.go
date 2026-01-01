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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	TracerName = "versitygw"
)

// OTELConfig holds OpenTelemetry configuration
type OTELConfig struct {
	// Enabled enables OTEL tracing
	Enabled bool
	// Endpoint is the OTLP endpoint (e.g., "localhost:4317" for gRPC, "localhost:4318" for HTTP)
	Endpoint string
	// ServiceName is the name of the service
	ServiceName string
	// ServiceVersion is the version of the service
	ServiceVersion string
	// Environment is the deployment environment (e.g., "production", "staging")
	Environment string
	// UseHTTP uses HTTP protocol instead of gRPC
	UseHTTP bool
	// Insecure disables TLS for the connection
	Insecure bool
	// SampleRate is the sampling rate (0.0 to 1.0, where 1.0 = 100%)
	SampleRate float64
}

var (
	tracer         trace.Tracer
	tracerProvider *sdktrace.TracerProvider
	otelEnabled    bool
)

// InitOTEL initializes OpenTelemetry tracing
func InitOTEL(ctx context.Context, cfg OTELConfig) error {
	if !cfg.Enabled || cfg.Endpoint == "" {
		otelEnabled = false
		tracer = otel.Tracer(TracerName)
		return nil
	}

	var exporter *otlptrace.Exporter
	var err error

	if cfg.UseHTTP {
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(cfg.Endpoint),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exporter, err = otlptracehttp.New(ctx, opts...)
	} else {
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptracegrpc.New(ctx, opts...)
	}

	if err != nil {
		return fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			attribute.String("environment", cfg.Environment),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	// Configure sampler
	var sampler sdktrace.Sampler
	if cfg.SampleRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if cfg.SampleRate <= 0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(cfg.SampleRate)
	}

	tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer = otel.Tracer(TracerName)
	otelEnabled = true

	return nil
}

// ShutdownOTEL gracefully shuts down the tracer provider
func ShutdownOTEL(ctx context.Context) error {
	if tracerProvider != nil {
		return tracerProvider.Shutdown(ctx)
	}
	return nil
}

// IsEnabled returns whether OTEL tracing is enabled
func IsEnabled() bool {
	return otelEnabled
}

// Tracer returns the global tracer
func Tracer() trace.Tracer {
	if tracer == nil {
		return otel.Tracer(TracerName)
	}
	return tracer
}

// StartSpan starts a new span with the given name
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, opts...)
}

// SpanFromContext returns the current span from the context
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// AddEvent adds an event to the current span
func AddEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(name, trace.WithAttributes(attrs...))
}

// SetError records an error on the current span
func SetError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// SetAttributes sets attributes on the current span
func SetAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attrs...)
}

// S3Attributes returns common S3 operation attributes
func S3Attributes(bucket, key, operation string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("s3.operation", operation),
	}
	if bucket != "" {
		attrs = append(attrs, attribute.String("s3.bucket", bucket))
	}
	if key != "" {
		attrs = append(attrs, attribute.String("s3.key", key))
	}
	return attrs
}

// MultipartAttributes returns multipart upload specific attributes
func MultipartAttributes(bucket, key, uploadID string, partNumber int32) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.key", key),
		attribute.String("s3.upload_id", uploadID),
		attribute.Int("s3.part_number", int(partNumber)),
	}
}

// TraceOperation wraps a function with tracing
func TraceOperation[T any](ctx context.Context, operationName string, attrs []attribute.KeyValue, fn func(context.Context) (T, error)) (T, error) {
	ctx, span := StartSpan(ctx, operationName, trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	result, err := fn(ctx)
	duration := time.Since(start)

	span.SetAttributes(attribute.Int64("duration_ms", duration.Milliseconds()))

	if err != nil {
		SetError(ctx, err)
	}

	return result, err
}
