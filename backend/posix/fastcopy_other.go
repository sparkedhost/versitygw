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

//go:build !linux
// +build !linux

package posix

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

// FastCopyConfig holds configuration for optimized file copying
type FastCopyConfig struct {
	// UseCopyFileRange is ignored on non-Linux (no copy_file_range syscall)
	UseCopyFileRange bool
	// BufferSize is the buffer size for io.Copy (default 1MB)
	BufferSize int
	// ParallelParts enables parallel part assembly for CompleteMultipartUpload
	ParallelParts bool
	// MaxParallelWorkers limits concurrent part copy operations
	MaxParallelWorkers int
	// UseDirectIO is ignored on non-Linux
	UseDirectIO bool
	// DirectIOThreshold is ignored on non-Linux
	DirectIOThreshold int64
	// PreallocateTarget is ignored on non-Linux (no fallocate)
	PreallocateTarget bool
}

// DefaultFastCopyConfig returns defaults for non-Linux systems
func DefaultFastCopyConfig() FastCopyConfig {
	return FastCopyConfig{
		UseCopyFileRange:   false,
		BufferSize:         4 * 1024 * 1024, // 4MB buffer
		ParallelParts:      false,           // Sequential on non-Linux for safety
		MaxParallelWorkers: 1,
		UseDirectIO:        false,
		DirectIOThreshold:  100 * 1024 * 1024,
		PreallocateTarget:  false,
	}
}

// FastFileCopy copies data between files using buffered I/O
func FastFileCopy(ctx context.Context, src, dst *os.File, size int64, cfg FastCopyConfig) (int64, error) {
	// Trace the overall fast copy operation (fallback mode on non-Linux)
	_, span := TracedFastFileCopy(ctx, size, false)
	defer span.End()

	return bufferedCopy(ctx, src, dst, size, cfg.BufferSize)
}

// bufferedCopy performs a buffered copy with context cancellation support
func bufferedCopy(ctx context.Context, src io.Reader, dst io.Writer, size int64, bufSize int) (int64, error) {
	if bufSize <= 0 {
		bufSize = 4 * 1024 * 1024 // 4MB default
	}

	buf := make([]byte, bufSize)
	var copied int64

	for {
		select {
		case <-ctx.Done():
			return copied, ctx.Err()
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			written, writeErr := dst.Write(buf[:n])
			copied += int64(written)
			if writeErr != nil {
				return copied, writeErr
			}
			if written != n {
				return copied, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return copied, nil
			}
			return copied, readErr
		}
	}
}

// PartCopyJob represents a part to be copied during CompleteMultipartUpload
type PartCopyJob struct {
	PartNumber int32
	SrcPath    string
	SrcSize    int64
	DstOffset  int64
}

// PartCopyResult holds the result of a part copy operation
type PartCopyResult struct {
	PartNumber  int32
	BytesCopied int64
	Error       error
}

// ParallelPartsCopy copies multiple parts to a destination file
// On non-Linux, this is always sequential
func ParallelPartsCopy(ctx context.Context, jobs []PartCopyJob, dst *os.File, cfg FastCopyConfig) (int64, error) {
	var totalCopied int64

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return totalCopied, ctx.Err()
		default:
		}

		src, err := os.Open(job.SrcPath)
		if err != nil {
			return totalCopied, fmt.Errorf("open part %d: %w", job.PartNumber, err)
		}

		// Seek destination to correct offset
		if _, err := dst.Seek(job.DstOffset, io.SeekStart); err != nil {
			src.Close()
			return totalCopied, fmt.Errorf("seek for part %d: %w", job.PartNumber, err)
		}

		copied, err := FastFileCopy(ctx, src, dst, job.SrcSize, cfg)
		src.Close()

		totalCopied += copied
		if err != nil {
			return totalCopied, fmt.Errorf("copy part %d: %w", job.PartNumber, err)
		}
	}

	return totalCopied, nil
}

// ProgressCallback is called during copy operations to report progress
type ProgressCallback func(bytesCopied, totalBytes int64)

// FastFileCopyWithProgress copies with progress reporting
func FastFileCopyWithProgress(ctx context.Context, src, dst *os.File, size int64, cfg FastCopyConfig, progress ProgressCallback) (int64, error) {
	if progress == nil {
		return FastFileCopy(ctx, src, dst, size, cfg)
	}

	buf := make([]byte, cfg.BufferSize)
	var copied int64
	lastReport := int64(0)
	reportInterval := size / 100
	if reportInterval < 1024*1024 {
		reportInterval = 1024 * 1024
	}

	for {
		select {
		case <-ctx.Done():
			return copied, ctx.Err()
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			written, writeErr := dst.Write(buf[:n])
			copied += int64(written)

			if copied-lastReport >= reportInterval {
				progress(copied, size)
				lastReport = copied
			}

			if writeErr != nil {
				return copied, writeErr
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				progress(copied, size)
				return copied, nil
			}
			return copied, readErr
		}
	}
}

// SyncFileData syncs file data to disk
func SyncFileData(f *os.File) error {
	return f.Sync()
}

// PrepareForSequentialRead is a no-op on non-Linux
func PrepareForSequentialRead(f *os.File, size int64) {}

// PrepareForSequentialWrite is a no-op on non-Linux
func PrepareForSequentialWrite(f *os.File, size int64) {}

// DropPageCache is a no-op on non-Linux
func DropPageCache(f *os.File, offset, length int64) {}

// CopyStats tracks copy operation statistics
type CopyStats struct {
	BytesCopied       int64
	PartsProcessed    int32
	CopyFileRangeUsed bool
	Duration          int64
}

// AtomicCopyStats is a thread-safe version of CopyStats
type AtomicCopyStats struct {
	bytesCopied    atomic.Int64
	partsProcessed atomic.Int32
	cfRangeUsed    atomic.Bool
	startTime      int64
}

func (s *AtomicCopyStats) AddBytes(n int64) {
	s.bytesCopied.Add(n)
}

func (s *AtomicCopyStats) AddPart() {
	s.partsProcessed.Add(1)
}

func (s *AtomicCopyStats) SetCopyFileRangeUsed() {
	s.cfRangeUsed.Store(true)
}

func (s *AtomicCopyStats) Stats() CopyStats {
	return CopyStats{
		BytesCopied:       s.bytesCopied.Load(),
		PartsProcessed:    s.partsProcessed.Load(),
		CopyFileRangeUsed: s.cfRangeUsed.Load(),
	}
}
