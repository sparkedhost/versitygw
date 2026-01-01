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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ErrorCategory represents the type of error for classification
type ErrorCategory string

const (
	ErrorCategoryFileSystem   ErrorCategory = "filesystem"
	ErrorCategoryPermission   ErrorCategory = "permission"
	ErrorCategoryNotFound     ErrorCategory = "not_found"
	ErrorCategoryQuota        ErrorCategory = "quota"
	ErrorCategoryNetwork      ErrorCategory = "network"
	ErrorCategoryTimeout      ErrorCategory = "timeout"
	ErrorCategoryConcurrency  ErrorCategory = "concurrency"
	ErrorCategoryDataIntegrity ErrorCategory = "data_integrity"
	ErrorCategoryInternal     ErrorCategory = "internal"
	ErrorCategoryClient       ErrorCategory = "client"
)

// ClassifyError categorizes an error for better handling and reporting
func ClassifyError(err error) ErrorCategory {
	if err == nil {
		return ""
	}

	// Check for specific error types
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, os.ErrNotExist) {
		return ErrorCategoryNotFound
	}
	if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
		return ErrorCategoryPermission
	}
	if errors.Is(err, syscall.EDQUOT) || errors.Is(err, syscall.ENOSPC) {
		return ErrorCategoryQuota
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorCategoryTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ErrorCategoryConcurrency
	}

	// Check error message for common patterns
	errMsg := strings.ToLower(err.Error())
	if strings.Contains(errMsg, "no such file") || strings.Contains(errMsg, "does not exist") {
		return ErrorCategoryNotFound
	}
	if strings.Contains(errMsg, "permission denied") || strings.Contains(errMsg, "access denied") {
		return ErrorCategoryPermission
	}
	if strings.Contains(errMsg, "quota") || strings.Contains(errMsg, "disk full") || strings.Contains(errMsg, "no space") {
		return ErrorCategoryQuota
	}
	if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "timed out") {
		return ErrorCategoryTimeout
	}
	if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "network") {
		return ErrorCategoryNetwork
	}
	if strings.Contains(errMsg, "checksum") || strings.Contains(errMsg, "corrupt") || strings.Contains(errMsg, "integrity") {
		return ErrorCategoryDataIntegrity
	}

	return ErrorCategoryInternal
}

// MultipartError wraps an error with multipart-specific context
type MultipartError struct {
	Err        error
	Bucket     string
	Key        string
	UploadID   string
	PartNumber int32
	PartPath   string
	Operation  string
	Category   ErrorCategory
	Timestamp  time.Time
	StackTrace string
}

func (e *MultipartError) Error() string {
	return fmt.Sprintf("multipart %s error: bucket=%s, key=%s, uploadID=%s, part=%d, path=%s: %v",
		e.Operation, e.Bucket, e.Key, e.UploadID, e.PartNumber, e.PartPath, e.Err)
}

func (e *MultipartError) Unwrap() error {
	return e.Err
}

// NewMultipartError creates a new MultipartError with full context
func NewMultipartError(err error, operation, bucket, key, uploadID string, partNumber int32, partPath string) *MultipartError {
	return &MultipartError{
		Err:        err,
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: partNumber,
		PartPath:   partPath,
		Operation:  operation,
		Category:   ClassifyError(err),
		Timestamp:  time.Now(),
		StackTrace: captureStackTrace(2),
	}
}

func captureStackTrace(skip int) string {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(skip+1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])

	var sb strings.Builder
	for {
		frame, more := frames.Next()
		fmt.Fprintf(&sb, "%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}
	return sb.String()
}

// ReportError reports an error to all configured observability backends
func ReportError(ctx context.Context, err error, attributes map[string]interface{}) {
	if err == nil {
		return
	}

	// Report to OTEL
	if IsEnabled() {
		span := trace.SpanFromContext(ctx)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		for k, v := range attributes {
			switch val := v.(type) {
			case string:
				span.SetAttributes(attribute.String(k, val))
			case int:
				span.SetAttributes(attribute.Int(k, val))
			case int64:
				span.SetAttributes(attribute.Int64(k, val))
			case bool:
				span.SetAttributes(attribute.Bool(k, val))
			}
		}
	}

	// Report to Sentry
	if IsSentryEnabled() {
		tags := make(map[string]string)
		extras := make(map[string]interface{})

		for k, v := range attributes {
			switch val := v.(type) {
			case string:
				tags[k] = val
			default:
				extras[k] = v
			}
		}

		CaptureError(err, tags, extras)
	}

	// Report to Prometheus
	if IsPrometheusEnabled() {
		category := string(ClassifyError(err))
		if bucket, ok := attributes["bucket"].(string); ok {
			if operation, ok := attributes["operation"].(string); ok {
				RecordBackendError("posix", operation, category)
				if strings.Contains(operation, "multipart") || strings.Contains(operation, "part") {
					RecordMultipartError(bucket, category)
				}
			}
		}
	}
}

// ReportMultipartError reports a multipart-specific error
func ReportMultipartError(ctx context.Context, mpErr *MultipartError) {
	if mpErr == nil {
		return
	}

	attributes := map[string]interface{}{
		"bucket":      mpErr.Bucket,
		"key":         mpErr.Key,
		"upload_id":   mpErr.UploadID,
		"part_number": mpErr.PartNumber,
		"part_path":   mpErr.PartPath,
		"operation":   mpErr.Operation,
		"category":    string(mpErr.Category),
		"timestamp":   mpErr.Timestamp.Format(time.RFC3339),
	}

	ReportError(ctx, mpErr.Err, attributes)

	// Additional Sentry context for multipart errors
	if IsSentryEnabled() {
		AddBreadcrumb("multipart", mpErr.Error(), sentry.LevelError, map[string]interface{}{
			"stack_trace": mpErr.StackTrace,
		})
	}
}

// EnsureDir ensures a directory exists, creating it if necessary with retry logic
func EnsureDir(path string, perm os.FileMode, maxRetries int) error {
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		// Check if directory already exists
		if info, err := os.Stat(path); err == nil {
			if info.IsDir() {
				return nil
			}
			return fmt.Errorf("path exists but is not a directory: %s", path)
		} else if !os.IsNotExist(err) {
			lastErr = err
			time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
			continue
		}

		// Try to create the directory
		if err := os.MkdirAll(path, perm); err != nil {
			if os.IsExist(err) {
				// Directory was created by another process
				return nil
			}
			lastErr = err
			time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to ensure directory after %d retries: %w", maxRetries, lastErr)
}

// ValidateMultipartPath validates and ensures the multipart upload path exists
func ValidateMultipartPath(bucket, objdir, uploadID string, partNumber int32) (string, error) {
	// Validate inputs
	if bucket == "" {
		return "", fmt.Errorf("bucket name is required")
	}
	if objdir == "" {
		return "", fmt.Errorf("object directory is required")
	}
	if uploadID == "" {
		return "", fmt.Errorf("upload ID is required")
	}
	if partNumber < 1 {
		return "", fmt.Errorf("part number must be >= 1, got %d", partNumber)
	}

	// Build the full path
	uploadPath := filepath.Join(bucket, objdir, uploadID)
	partPath := filepath.Join(uploadPath, fmt.Sprintf("%d", partNumber))

	// Ensure the upload directory exists
	if err := EnsureDir(uploadPath, 0755, 3); err != nil {
		return "", fmt.Errorf("ensure upload directory: %w", err)
	}

	return partPath, nil
}

// SafeOpenPartFile opens a part file with validation and error handling
func SafeOpenPartFile(ctx context.Context, partPath string, flags int, perm os.FileMode) (*os.File, error) {
	ctx, span := StartSpan(ctx, "SafeOpenPartFile")
	defer span.End()

	span.SetAttributes(attribute.String("path", partPath))

	// Ensure parent directory exists
	parentDir := filepath.Dir(partPath)
	if err := EnsureDir(parentDir, 0755, 3); err != nil {
		mpErr := NewMultipartError(err, "ensure_parent_dir", "", "", "", 0, partPath)
		ReportMultipartError(ctx, mpErr)
		return nil, fmt.Errorf("ensure parent directory %s: %w", parentDir, err)
	}

	// Open the file
	f, err := os.OpenFile(partPath, flags, perm)
	if err != nil {
		mpErr := NewMultipartError(err, "open_part_file", "", "", "", 0, partPath)
		ReportMultipartError(ctx, mpErr)
		return nil, fmt.Errorf("open part file %s: %w", partPath, err)
	}

	span.SetAttributes(attribute.String("status", "success"))
	return f, nil
}

// SafeReadPartFile reads a part file with comprehensive error handling
func SafeReadPartFile(ctx context.Context, bucket, key, uploadID string, partNumber int32, partPath string) (*os.File, os.FileInfo, error) {
	ctx, span := StartSpan(ctx, "SafeReadPartFile")
	defer span.End()

	span.SetAttributes(
		attribute.String("bucket", bucket),
		attribute.String("key", key),
		attribute.String("upload_id", uploadID),
		attribute.Int("part_number", int(partNumber)),
		attribute.String("path", partPath),
	)

	// Check if file exists
	fi, err := os.Stat(partPath)
	if err != nil {
		if os.IsNotExist(err) {
			mpErr := NewMultipartError(err, "stat_part", bucket, key, uploadID, partNumber, partPath)
			ReportMultipartError(ctx, mpErr)
			return nil, nil, fmt.Errorf("part %d does not exist at %s: %w", partNumber, partPath, err)
		}
		mpErr := NewMultipartError(err, "stat_part", bucket, key, uploadID, partNumber, partPath)
		ReportMultipartError(ctx, mpErr)
		return nil, nil, fmt.Errorf("stat part %d at %s: %w", partNumber, partPath, err)
	}

	// Open the file
	f, err := os.Open(partPath)
	if err != nil {
		mpErr := NewMultipartError(err, "open_part", bucket, key, uploadID, partNumber, partPath)
		ReportMultipartError(ctx, mpErr)
		return nil, nil, fmt.Errorf("open part %d at %s: %w", partNumber, partPath, err)
	}

	span.SetAttributes(
		attribute.Int64("size", fi.Size()),
		attribute.String("status", "success"),
	)

	return f, fi, nil
}

// DiagnoseMultipartIssue provides diagnostic information for multipart issues
func DiagnoseMultipartIssue(bucket, objdir, uploadID string) map[string]interface{} {
	diagnosis := map[string]interface{}{
		"bucket":       bucket,
		"objdir":       objdir,
		"upload_id":    uploadID,
		"timestamp":    time.Now().Format(time.RFC3339),
		"goroutines":   runtime.NumGoroutine(),
	}

	// Check bucket path
	bucketPath := bucket
	if info, err := os.Stat(bucketPath); err != nil {
		diagnosis["bucket_status"] = fmt.Sprintf("error: %v", err)
	} else {
		diagnosis["bucket_status"] = "exists"
		diagnosis["bucket_is_dir"] = info.IsDir()
		diagnosis["bucket_mode"] = info.Mode().String()
	}

	// Check multipart directory
	mpDir := filepath.Join(bucket, objdir)
	if info, err := os.Stat(mpDir); err != nil {
		diagnosis["mp_dir_status"] = fmt.Sprintf("error: %v", err)
	} else {
		diagnosis["mp_dir_status"] = "exists"
		diagnosis["mp_dir_mode"] = info.Mode().String()
	}

	// Check upload directory
	uploadDir := filepath.Join(bucket, objdir, uploadID)
	if info, err := os.Stat(uploadDir); err != nil {
		diagnosis["upload_dir_status"] = fmt.Sprintf("error: %v", err)
	} else {
		diagnosis["upload_dir_status"] = "exists"
		diagnosis["upload_dir_mode"] = info.Mode().String()

		// List parts
		entries, err := os.ReadDir(uploadDir)
		if err != nil {
			diagnosis["parts_list_error"] = err.Error()
		} else {
			parts := make([]string, 0, len(entries))
			for _, e := range entries {
				parts = append(parts, e.Name())
			}
			diagnosis["parts"] = parts
			diagnosis["parts_count"] = len(parts)
		}
	}

	// Check disk space
	var stat syscall.Statfs_t
	if err := syscall.Statfs(bucket, &stat); err == nil {
		diagnosis["disk_available_bytes"] = stat.Bavail * uint64(stat.Bsize)
		diagnosis["disk_total_bytes"] = stat.Blocks * uint64(stat.Bsize)
	}

	return diagnosis
}
