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

package posix

import (
	"context"
	"time"

	"github.com/versity/versitygw/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Ensure trace.Span interface is used (for compile-time check)
var _ trace.Span = (trace.Span)(nil)

// Internal span names for posix operations
const (
	spanOpenTmpFile         = "Posix.IO.OpenTmpFile"
	spanWriteData           = "Posix.IO.WriteData"
	spanReadPartFile        = "Posix.IO.ReadPartFile"
	spanFastFileCopy        = "Posix.IO.FastFileCopy"
	spanCopyFileRange       = "Posix.IO.CopyFileRange"
	spanLinkFile            = "Posix.IO.LinkFile"
	spanFalloc              = "Posix.IO.Falloc"
	spanSequentialHint      = "Posix.IO.SequentialHint"
	spanDropPageCache       = "Posix.IO.DropPageCache"
	spanAssembleParts       = "Posix.IO.AssembleParts"
	spanCopyPart            = "Posix.IO.CopyPart"
	spanChecksumComputation = "Posix.IO.ChecksumComputation"
)

// startIOSpan starts an internal span for I/O operations
func startIOSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !observability.IsEnabled() {
		return ctx, trace.SpanFromContext(ctx) // return existing (possibly noop) span
	}
	return observability.StartInternalSpan(ctx, name, attrs...)
}

// TracedTmpFileOpen traces the openTmpFile operation
func TracedTmpFileOpen(ctx context.Context, dir, bucket, obj string, size int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanOpenTmpFile,
		attribute.String("posix.dir", dir),
		attribute.String("posix.bucket", bucket),
		attribute.String("posix.object", obj),
		attribute.Int64("posix.size", size),
	)
}

// TracedWriteData traces the data write operation during upload
func TracedWriteData(ctx context.Context, bucket, object string, size int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanWriteData,
		attribute.String("posix.bucket", bucket),
		attribute.String("posix.object", object),
		attribute.Int64("posix.size", size),
	)
}

// TracedReadPartFile traces reading a part file during CompleteMultipartUpload
func TracedReadPartFile(ctx context.Context, bucket, object, uploadID string, partNumber int32, partPath string, partSize int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanReadPartFile,
		attribute.String("posix.bucket", bucket),
		attribute.String("posix.object", object),
		attribute.String("posix.upload_id", uploadID),
		attribute.Int("posix.part_number", int(partNumber)),
		attribute.String("posix.part_path", partPath),
		attribute.Int64("posix.part_size", partSize),
	)
}

// TracedFastFileCopy traces the FastFileCopy operation
func TracedFastFileCopy(ctx context.Context, size int64, useCopyFileRange bool) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanFastFileCopy,
		attribute.Int64("posix.size", size),
		attribute.Bool("posix.use_copy_file_range", useCopyFileRange),
	)
}

// TracedCopyFileRange traces the copy_file_range syscall
func TracedCopyFileRange(ctx context.Context, size int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanCopyFileRange,
		attribute.Int64("posix.size", size),
	)
}

// TracedLinkFile traces the file linking operation
func TracedLinkFile(ctx context.Context, bucket, object string) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanLinkFile,
		attribute.String("posix.bucket", bucket),
		attribute.String("posix.object", object),
	)
}

// TracedFalloc traces the falloc/preallocation operation
func TracedFalloc(ctx context.Context, size int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanFalloc,
		attribute.Int64("posix.size", size),
	)
}

// TracedSequentialHint traces kernel I/O hints
func TracedSequentialHint(ctx context.Context, operation string, size int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanSequentialHint,
		attribute.String("posix.hint_operation", operation),
		attribute.Int64("posix.size", size),
	)
}

// TracedDropPageCache traces page cache drop operations
func TracedDropPageCache(ctx context.Context, offset, size int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanDropPageCache,
		attribute.Int64("posix.offset", offset),
		attribute.Int64("posix.size", size),
	)
}

// TracedAssembleParts traces the overall part assembly operation
func TracedAssembleParts(ctx context.Context, bucket, object, uploadID string, totalParts int, totalSize int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanAssembleParts,
		attribute.String("posix.bucket", bucket),
		attribute.String("posix.object", object),
		attribute.String("posix.upload_id", uploadID),
		attribute.Int("posix.total_parts", totalParts),
		attribute.Int64("posix.total_size", totalSize),
	)
}

// TracedCopyPart traces copying a single part to the final object
func TracedCopyPart(ctx context.Context, partNumber int, partSize int64, method string) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanCopyPart,
		attribute.Int("posix.part_number", partNumber),
		attribute.Int64("posix.part_size", partSize),
		attribute.String("posix.copy_method", method),
	)
}

// TracedChecksumComputation traces checksum computation
func TracedChecksumComputation(ctx context.Context, algorithm string, size int64) (context.Context, trace.Span) {
	return startIOSpan(ctx, spanChecksumComputation,
		attribute.String("posix.checksum_algorithm", algorithm),
		attribute.Int64("posix.size", size),
	)
}

// RecordIOMetrics records I/O metrics on a span
func RecordIOMetrics(span trace.Span, bytesWritten, bytesRead int64, duration time.Duration) {
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.Int64("posix.bytes_written", bytesWritten),
		attribute.Int64("posix.bytes_read", bytesRead),
		attribute.Int64("posix.duration_ms", duration.Milliseconds()),
	)
	if duration > 0 {
		// Calculate throughput in MB/s
		totalBytes := bytesWritten + bytesRead
		if totalBytes > 0 {
			throughputMBps := float64(totalBytes) / duration.Seconds() / (1024 * 1024)
			span.SetAttributes(attribute.Float64("posix.throughput_mbps", throughputMBps))
		}
	}
}

// RecordCopyFileRangeResult records the result of copy_file_range
func RecordCopyFileRangeResult(span trace.Span, bytesCopied int64, usedFallback bool, err error) {
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.Int64("posix.bytes_copied", bytesCopied),
		attribute.Bool("posix.used_fallback", usedFallback),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
}

// RecordPartAssemblyProgress records progress in part assembly
func RecordPartAssemblyProgress(span trace.Span, currentPart, totalParts int, bytesCopied int64) {
	if span == nil {
		return
	}
	span.AddEvent("part_assembled",
		trace.WithAttributes(
			attribute.Int("posix.current_part", currentPart),
			attribute.Int("posix.total_parts", totalParts),
			attribute.Int64("posix.bytes_copied", bytesCopied),
		),
	)
}
