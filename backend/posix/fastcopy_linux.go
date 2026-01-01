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

//go:build linux
// +build linux

package posix

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// FastCopyConfig holds configuration for optimized file copying
type FastCopyConfig struct {
	// UseCopyFileRange enables copy_file_range syscall (kernel-space copy)
	UseCopyFileRange bool
	// BufferSize is the buffer size for fallback io.Copy (default 1MB)
	BufferSize int
	// ParallelParts enables parallel part assembly for CompleteMultipartUpload
	ParallelParts bool
	// MaxParallelWorkers limits concurrent part copy operations
	MaxParallelWorkers int
	// UseDirectIO enables O_DIRECT for large files (bypasses page cache)
	UseDirectIO bool
	// DirectIOThreshold is the minimum file size to use O_DIRECT (default 100MB)
	DirectIOThreshold int64
	// PreallocateTarget preallocates target file space before copying
	PreallocateTarget bool
}

// DefaultFastCopyConfig returns optimized defaults for EXT4
func DefaultFastCopyConfig() FastCopyConfig {
	return FastCopyConfig{
		UseCopyFileRange:   true,
		BufferSize:         4 * 1024 * 1024, // 4MB buffer
		ParallelParts:      true,
		MaxParallelWorkers: 4,
		UseDirectIO:        false, // Disabled by default, can cause issues
		DirectIOThreshold:  100 * 1024 * 1024,
		PreallocateTarget:  true,
	}
}

// copyFileRange uses the copy_file_range syscall for zero-copy between files
// This is significantly faster on EXT4 as it happens entirely in kernel space
func copyFileRange(src, dst *os.File, srcOffset, dstOffset, length int64) (int64, error) {
	var copied int64
	
	for copied < length {
		// copy_file_range can copy up to 2GB at a time on most systems
		toCopy := length - copied
		if toCopy > 1<<30 { // 1GB chunks
			toCopy = 1 << 30
		}

		srcOff := srcOffset + copied
		dstOff := dstOffset + copied

		n, err := unix.CopyFileRange(int(src.Fd()), &srcOff, int(dst.Fd()), &dstOff, int(toCopy), 0)
		if err != nil {
			if err == unix.EXDEV || err == unix.ENOSYS || err == unix.EOPNOTSUPP {
				// Cross-device, syscall not supported, or fs doesn't support it
				// Fall back to regular copy for remaining data
				return copied, err
			}
			return copied, fmt.Errorf("copy_file_range: %w", err)
		}
		if n == 0 {
			break
		}
		copied += int64(n)
	}

	return copied, nil
}

// FastFileCopy copies data between files using the most efficient method available
func FastFileCopy(ctx context.Context, src, dst *os.File, size int64, cfg FastCopyConfig) (int64, error) {
	// Trace the overall fast copy operation
	_, span := TracedFastFileCopy(ctx, size, cfg.UseCopyFileRange)
	defer span.End()

	// Try copy_file_range first (zero-copy in kernel)
	if cfg.UseCopyFileRange && size > 0 {
		// Trace the copy_file_range syscall
		_, cfrSpan := TracedCopyFileRange(ctx, size)
		copied, err := copyFileRange(src, dst, 0, 0, size)
		if err == nil {
			RecordCopyFileRangeResult(cfrSpan, copied, false, nil)
			cfrSpan.End()
			return copied, nil
		}
		// If copy_file_range failed but copied some data, we need to handle partial copy
		if copied > 0 {
			RecordCopyFileRangeResult(cfrSpan, copied, true, err)
			cfrSpan.End()
			// Seek both files to continue with fallback
			src.Seek(copied, io.SeekStart)
			dst.Seek(copied, io.SeekStart)
			
			// Fall through to regular copy for remaining data
			remaining := size - copied
			n, err := bufferedCopy(ctx, src, dst, remaining, cfg.BufferSize)
			return copied + n, err
		}
		RecordCopyFileRangeResult(cfrSpan, 0, true, err)
		cfrSpan.End()
		// Fall through to buffered copy
	}

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
	PartNumber int32
	BytesCopied int64
	Error      error
}

// ParallelPartsCopy copies multiple parts to a destination file in parallel
// This is optimized for large multipart uploads on EXT4 where individual
// part files need to be assembled into a single object
func ParallelPartsCopy(ctx context.Context, jobs []PartCopyJob, dst *os.File, cfg FastCopyConfig) (int64, error) {
	if len(jobs) == 0 {
		return 0, nil
	}

	// For small number of parts or if parallel disabled, use sequential
	if !cfg.ParallelParts || len(jobs) <= 2 {
		return sequentialPartsCopy(ctx, jobs, dst, cfg)
	}

	workers := cfg.MaxParallelWorkers
	if workers <= 0 {
		workers = 4
	}
	if workers > len(jobs) {
		workers = len(jobs)
	}

	// Preallocate the target file if configured
	if cfg.PreallocateTarget {
		var totalSize int64
		for _, job := range jobs {
			totalSize += job.SrcSize
		}
		if totalSize > 0 {
			// fallocate is best-effort
			unix.Fallocate(int(dst.Fd()), 0, 0, totalSize)
		}
	}

	// Create job channel and result channel
	jobChan := make(chan PartCopyJob, len(jobs))
	resultChan := make(chan PartCopyResult, len(jobs))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobChan {
				result := copyPart(ctx, job, dst, cfg)
				resultChan <- result
			}
		}()
	}

	// Send jobs
	for _, job := range jobs {
		jobChan <- job
	}
	close(jobChan)

	// Wait for workers and close result channel
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var totalCopied int64
	var firstErr error
	for result := range resultChan {
		if result.Error != nil && firstErr == nil {
			firstErr = fmt.Errorf("part %d: %w", result.PartNumber, result.Error)
		}
		totalCopied += result.BytesCopied
	}

	return totalCopied, firstErr
}

func copyPart(ctx context.Context, job PartCopyJob, dst *os.File, cfg FastCopyConfig) PartCopyResult {
	result := PartCopyResult{PartNumber: job.PartNumber}

	src, err := os.Open(job.SrcPath)
	if err != nil {
		result.Error = fmt.Errorf("open part file: %w", err)
		return result
	}
	defer src.Close()

	// Use pwrite to write at specific offset (thread-safe)
	if cfg.UseCopyFileRange && job.SrcSize > 0 {
		srcOffset := int64(0)
		dstOffset := job.DstOffset
		
		copied, err := unix.CopyFileRange(
			int(src.Fd()), &srcOffset,
			int(dst.Fd()), &dstOffset,
			int(job.SrcSize), 0,
		)
		if err == nil {
			result.BytesCopied = int64(copied)
			return result
		}
		// Fall through to buffered copy on error
	}

	// Fallback: read into buffer, pwrite to destination
	buf := make([]byte, cfg.BufferSize)
	var copied int64
	offset := job.DstOffset

	for copied < job.SrcSize {
		select {
		case <-ctx.Done():
			result.Error = ctx.Err()
			result.BytesCopied = copied
			return result
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			written, writeErr := unix.Pwrite(int(dst.Fd()), buf[:n], offset)
			if writeErr != nil {
				result.Error = writeErr
				result.BytesCopied = copied
				return result
			}
			copied += int64(written)
			offset += int64(written)
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			result.Error = readErr
			result.BytesCopied = copied
			return result
		}
	}

	result.BytesCopied = copied
	return result
}

func sequentialPartsCopy(ctx context.Context, jobs []PartCopyJob, dst *os.File, cfg FastCopyConfig) (int64, error) {
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

	// For progress tracking, we need to use buffered copy
	buf := make([]byte, cfg.BufferSize)
	var copied int64
	lastReport := int64(0)
	reportInterval := size / 100 // Report every 1%
	if reportInterval < 1024*1024 {
		reportInterval = 1024 * 1024 // At least every 1MB
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
				progress(copied, size) // Final report
				return copied, nil
			}
			return copied, readErr
		}
	}
}

// SyncFileData syncs file data to disk (fdatasync)
// This is faster than Sync() as it doesn't sync metadata
func SyncFileData(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}

// AdviseFAdvise gives the kernel hints about file access patterns
func AdviseFAdvise(f *os.File, offset, length int64, advice int) error {
	return unix.Fadvise(int(f.Fd()), offset, length, advice)
}

// PrepareForSequentialRead hints to kernel that file will be read sequentially
func PrepareForSequentialRead(f *os.File, size int64) {
	unix.Fadvise(int(f.Fd()), 0, size, unix.FADV_SEQUENTIAL)
	unix.Fadvise(int(f.Fd()), 0, size, unix.FADV_WILLNEED)
}

// PrepareForSequentialWrite hints to kernel that file will be written sequentially
func PrepareForSequentialWrite(f *os.File, size int64) {
	unix.Fadvise(int(f.Fd()), 0, size, unix.FADV_SEQUENTIAL)
}

// DropPageCache drops page cache for a file after reading (reduces memory pressure)
func DropPageCache(f *os.File, offset, length int64) {
	unix.Fadvise(int(f.Fd()), offset, length, unix.FADV_DONTNEED)
}

// CopyStats tracks copy operation statistics
type CopyStats struct {
	BytesCopied      int64
	PartsProcessed   int32
	CopyFileRangeUsed bool
	Duration         int64 // nanoseconds
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
