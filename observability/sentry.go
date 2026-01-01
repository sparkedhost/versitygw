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
	"runtime"
	"time"

	"github.com/getsentry/sentry-go"
)

// SentryConfig holds Sentry configuration
type SentryConfig struct {
	// Enabled enables Sentry error reporting
	Enabled bool
	// DSN is the Sentry DSN
	DSN string
	// Environment is the deployment environment
	Environment string
	// Release is the application version/release
	Release string
	// SampleRate is the error sample rate (0.0 to 1.0)
	SampleRate float64
	// TracesSampleRate is the traces sample rate for performance monitoring (0.0 to 1.0)
	TracesSampleRate float64
	// Debug enables Sentry debug mode
	Debug bool
	// ServerName is the server name to report
	ServerName string
}

var sentryEnabled bool

// InitSentry initializes the Sentry SDK
func InitSentry(cfg SentryConfig) error {
	if !cfg.Enabled || cfg.DSN == "" {
		sentryEnabled = false
		return nil
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn:              cfg.DSN,
		Environment:      cfg.Environment,
		Release:          cfg.Release,
		SampleRate:       cfg.SampleRate,
		TracesSampleRate: cfg.TracesSampleRate,
		Debug:            cfg.Debug,
		ServerName:       cfg.ServerName,
		BeforeSend: func(event *sentry.Event, hint *sentry.EventHint) *sentry.Event {
			// Add additional context or filter events here
			return event
		},
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Sentry: %w", err)
	}

	sentryEnabled = true
	return nil
}

// ShutdownSentry flushes any buffered events
func ShutdownSentry(timeout time.Duration) {
	if sentryEnabled {
		sentry.Flush(timeout)
	}
}

// IsSentryEnabled returns whether Sentry is enabled
func IsSentryEnabled() bool {
	return sentryEnabled
}

// CaptureError captures an error and sends it to Sentry
func CaptureError(err error, tags map[string]string, extras map[string]interface{}) {
	if !sentryEnabled || err == nil {
		return
	}

	sentry.WithScope(func(scope *sentry.Scope) {
		for k, v := range tags {
			scope.SetTag(k, v)
		}
		for k, v := range extras {
			scope.SetExtra(k, v)
		}
		sentry.CaptureException(err)
	})
}

// CaptureMessage captures a message and sends it to Sentry
func CaptureMessage(message string, level sentry.Level, tags map[string]string) {
	if !sentryEnabled {
		return
	}

	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetLevel(level)
		for k, v := range tags {
			scope.SetTag(k, v)
		}
		sentry.CaptureMessage(message)
	})
}

// CaptureS3Error captures an S3-specific error with context
func CaptureS3Error(err error, operation, bucket, key, uploadID string, partNumber int32) {
	if !sentryEnabled || err == nil {
		return
	}

	tags := map[string]string{
		"s3.operation": operation,
	}
	if bucket != "" {
		tags["s3.bucket"] = bucket
	}

	extras := map[string]interface{}{
		"s3.key":         key,
		"s3.upload_id":   uploadID,
		"s3.part_number": partNumber,
	}

	CaptureError(err, tags, extras)
}

// CaptureMultipartError captures a multipart upload error with full context
func CaptureMultipartError(err error, bucket, key, uploadID string, partNumber int32, partPath string) {
	if !sentryEnabled || err == nil {
		return
	}

	tags := map[string]string{
		"s3.operation": "multipart_upload",
		"s3.bucket":    bucket,
		"error.type":   "multipart_error",
	}

	extras := map[string]interface{}{
		"s3.key":         key,
		"s3.upload_id":   uploadID,
		"s3.part_number": partNumber,
		"part_path":      partPath,
		"go.goroutines":  runtime.NumGoroutine(),
	}

	CaptureError(err, tags, extras)
}

// StartTransaction starts a Sentry transaction for performance monitoring
func StartTransaction(ctx context.Context, operation, name string) (*sentry.Span, context.Context) {
	if !sentryEnabled {
		return nil, ctx
	}

	options := []sentry.SpanOption{
		sentry.WithOpName(operation),
	}

	span := sentry.StartSpan(ctx, operation, options...)
	span.Description = name

	return span, span.Context()
}

// StartSpanFromContext starts a child span from the current transaction
func StartSentrySpan(ctx context.Context, operation, description string) *sentry.Span {
	if !sentryEnabled {
		return nil
	}

	span := sentry.StartSpan(ctx, operation)
	span.Description = description
	return span
}

// FinishSpan finishes a Sentry span
func FinishSpan(span *sentry.Span) {
	if span != nil {
		span.Finish()
	}
}

// SetSpanData sets data on a Sentry span
func SetSpanData(span *sentry.Span, key string, value string) {
	if span != nil {
		span.SetData(key, value)
	}
}

// SetSpanStatus sets the status on a Sentry span
func SetSpanStatus(span *sentry.Span, status sentry.SpanStatus) {
	if span != nil {
		span.Status = status
	}
}

// RecoverAndCapture recovers from a panic and captures it to Sentry
func RecoverAndCapture(extras map[string]interface{}) {
	if !sentryEnabled {
		return
	}

	if r := recover(); r != nil {
		var err error
		switch v := r.(type) {
		case error:
			err = v
		default:
			err = fmt.Errorf("panic: %v", v)
		}

		sentry.WithScope(func(scope *sentry.Scope) {
			for k, v := range extras {
				scope.SetExtra(k, v)
			}
			scope.SetLevel(sentry.LevelFatal)
			sentry.CaptureException(err)
		})

		// Re-panic after capturing
		panic(r)
	}
}

// AddBreadcrumb adds a breadcrumb to the current scope
func AddBreadcrumb(category, message string, level sentry.Level, data map[string]interface{}) {
	if !sentryEnabled {
		return
	}

	sentry.AddBreadcrumb(&sentry.Breadcrumb{
		Category:  category,
		Message:   message,
		Level:     level,
		Data:      data,
		Timestamp: time.Now(),
	})
}

// SetUser sets user information for the current scope
func SetUser(id, username, email string) {
	if !sentryEnabled {
		return
	}

	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetUser(sentry.User{
			ID:       id,
			Username: username,
			Email:    email,
		})
	})
}
