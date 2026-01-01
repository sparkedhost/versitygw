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

package backend

import (
	"bufio"
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/observability"
	"github.com/versity/versitygw/s3response"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TracingBackend wraps a Backend with OpenTelemetry tracing
type TracingBackend struct {
	backend Backend
	name    string
}

// NewTracingBackend creates a new tracing wrapper for a backend
func NewTracingBackend(be Backend) *TracingBackend {
	return &TracingBackend{
		backend: be,
		name:    be.String(),
	}
}

// String returns the backend name
func (t *TracingBackend) String() string {
	return t.backend.String()
}

// Shutdown shuts down the backend
func (t *TracingBackend) Shutdown() {
	t.backend.Shutdown()
}

func (t *TracingBackend) startSpan(ctx context.Context, operation string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	allAttrs := append([]attribute.KeyValue{
		attribute.String("backend.type", t.name),
		attribute.String("backend.operation", operation),
	}, attrs...)

	return observability.Tracer().Start(ctx, "Backend."+operation,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(allAttrs...),
	)
}

func (t *TracingBackend) endSpan(span trace.Span, start time.Time, err error) {
	span.SetAttributes(attribute.Int64("duration_ms", time.Since(start).Milliseconds()))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

// Bucket Operations

func (t *TracingBackend) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	ctx, span := t.startSpan(ctx, "ListBuckets")
	start := time.Now()
	result, err := t.backend.ListBuckets(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	ctx, span := t.startSpan(ctx, "HeadBucket",
		attribute.String("s3.bucket", *input.Bucket))
	start := time.Now()
	result, err := t.backend.HeadBucket(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	ctx, span := t.startSpan(ctx, "GetBucketAcl",
		attribute.String("s3.bucket", *input.Bucket))
	start := time.Now()
	result, err := t.backend.GetBucketAcl(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, defaultACL []byte) error {
	ctx, span := t.startSpan(ctx, "CreateBucket",
		attribute.String("s3.bucket", *input.Bucket))
	start := time.Now()
	err := t.backend.CreateBucket(ctx, input, defaultACL)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) PutBucketAcl(ctx context.Context, bucket string, data []byte) error {
	ctx, span := t.startSpan(ctx, "PutBucketAcl",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.PutBucketAcl(ctx, bucket, data)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) DeleteBucket(ctx context.Context, bucket string) error {
	ctx, span := t.startSpan(ctx, "DeleteBucket",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.DeleteBucket(ctx, bucket)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	ctx, span := t.startSpan(ctx, "PutBucketVersioning",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.versioning_status", string(status)))
	start := time.Now()
	err := t.backend.PutBucketVersioning(ctx, bucket, status)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	ctx, span := t.startSpan(ctx, "GetBucketVersioning",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	result, err := t.backend.GetBucketVersioning(ctx, bucket)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	ctx, span := t.startSpan(ctx, "PutBucketPolicy",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.PutBucketPolicy(ctx, bucket, policy)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	ctx, span := t.startSpan(ctx, "GetBucketPolicy",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	result, err := t.backend.GetBucketPolicy(ctx, bucket)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	ctx, span := t.startSpan(ctx, "DeleteBucketPolicy",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.DeleteBucketPolicy(ctx, bucket)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) PutBucketOwnershipControls(ctx context.Context, bucket string, ownership types.ObjectOwnership) error {
	ctx, span := t.startSpan(ctx, "PutBucketOwnershipControls",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.PutBucketOwnershipControls(ctx, bucket, ownership)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) GetBucketOwnershipControls(ctx context.Context, bucket string) (types.ObjectOwnership, error) {
	ctx, span := t.startSpan(ctx, "GetBucketOwnershipControls",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	result, err := t.backend.GetBucketOwnershipControls(ctx, bucket)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	ctx, span := t.startSpan(ctx, "DeleteBucketOwnershipControls",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.DeleteBucketOwnershipControls(ctx, bucket)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) PutBucketCors(ctx context.Context, bucket string, cors []byte) error {
	ctx, span := t.startSpan(ctx, "PutBucketCors",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.PutBucketCors(ctx, bucket, cors)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) GetBucketCors(ctx context.Context, bucket string) ([]byte, error) {
	ctx, span := t.startSpan(ctx, "GetBucketCors",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	result, err := t.backend.GetBucketCors(ctx, bucket)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) DeleteBucketCors(ctx context.Context, bucket string) error {
	ctx, span := t.startSpan(ctx, "DeleteBucketCors",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.DeleteBucketCors(ctx, bucket)
	t.endSpan(span, start, err)
	return err
}

// Multipart Operations

func (t *TracingBackend) CreateMultipartUpload(ctx context.Context, input s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	attrs := []attribute.KeyValue{}
	if input.Bucket != nil {
		attrs = append(attrs, attribute.String("s3.bucket", *input.Bucket))
	}
	if input.Key != nil {
		attrs = append(attrs, attribute.String("s3.key", *input.Key))
	}
	ctx, span := t.startSpan(ctx, "CreateMultipartUpload", attrs...)
	start := time.Now()
	result, err := t.backend.CreateMultipartUpload(ctx, input)
	if err == nil {
		span.SetAttributes(attribute.String("s3.upload_id", result.UploadId))
	}
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	attrs := []attribute.KeyValue{
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key),
	}
	if input.UploadId != nil {
		attrs = append(attrs, attribute.String("s3.upload_id", *input.UploadId))
	}
	if input.MultipartUpload != nil {
		attrs = append(attrs, attribute.Int("s3.part_count", len(input.MultipartUpload.Parts)))
	}
	ctx, span := t.startSpan(ctx, "CompleteMultipartUpload", attrs...)
	start := time.Now()
	result, versionId, err := t.backend.CompleteMultipartUpload(ctx, input)
	if versionId != "" {
		span.SetAttributes(attribute.String("s3.version_id", versionId))
	}
	t.endSpan(span, start, err)
	return result, versionId, err
}

func (t *TracingBackend) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) error {
	ctx, span := t.startSpan(ctx, "AbortMultipartUpload",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key),
		attribute.String("s3.upload_id", *input.UploadId))
	start := time.Now()
	err := t.backend.AbortMultipartUpload(ctx, input)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) ListMultipartUploads(ctx context.Context, input *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	ctx, span := t.startSpan(ctx, "ListMultipartUploads",
		attribute.String("s3.bucket", *input.Bucket))
	start := time.Now()
	result, err := t.backend.ListMultipartUploads(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) ListParts(ctx context.Context, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	ctx, span := t.startSpan(ctx, "ListParts",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key),
		attribute.String("s3.upload_id", *input.UploadId))
	start := time.Now()
	result, err := t.backend.ListParts(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	attrs := []attribute.KeyValue{
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key),
		attribute.String("s3.upload_id", *input.UploadId),
		attribute.Int("s3.part_number", int(*input.PartNumber)),
	}
	if input.ContentLength != nil {
		attrs = append(attrs, attribute.Int64("s3.content_length", *input.ContentLength))
	}
	ctx, span := t.startSpan(ctx, "UploadPart", attrs...)
	start := time.Now()
	result, err := t.backend.UploadPart(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) UploadPartCopy(ctx context.Context, input *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	ctx, span := t.startSpan(ctx, "UploadPartCopy",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key),
		attribute.String("s3.upload_id", *input.UploadId),
		attribute.Int("s3.part_number", int(*input.PartNumber)),
		attribute.String("s3.copy_source", *input.CopySource))
	start := time.Now()
	result, err := t.backend.UploadPartCopy(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

// Object Operations

func (t *TracingBackend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	attrs := []attribute.KeyValue{}
	if input.Bucket != nil {
		attrs = append(attrs, attribute.String("s3.bucket", *input.Bucket))
	}
	if input.Key != nil {
		attrs = append(attrs, attribute.String("s3.key", *input.Key))
	}
	if input.ContentLength != nil {
		attrs = append(attrs, attribute.Int64("s3.content_length", *input.ContentLength))
	}
	ctx, span := t.startSpan(ctx, "PutObject", attrs...)
	start := time.Now()
	result, err := t.backend.PutObject(ctx, input)
	if result.ETag != "" {
		span.SetAttributes(attribute.String("s3.etag", result.ETag))
	}
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	ctx, span := t.startSpan(ctx, "HeadObject",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key))
	start := time.Now()
	result, err := t.backend.HeadObject(ctx, input)
	if err == nil && result.ContentLength != nil {
		span.SetAttributes(attribute.Int64("s3.content_length", *result.ContentLength))
	}
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	ctx, span := t.startSpan(ctx, "GetObject",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key))
	start := time.Now()
	result, err := t.backend.GetObject(ctx, input)
	if err == nil && result.ContentLength != nil {
		span.SetAttributes(attribute.Int64("s3.content_length", *result.ContentLength))
	}
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) GetObjectAcl(ctx context.Context, input *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	ctx, span := t.startSpan(ctx, "GetObjectAcl",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key))
	start := time.Now()
	result, err := t.backend.GetObjectAcl(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) GetObjectAttributes(ctx context.Context, input *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	ctx, span := t.startSpan(ctx, "GetObjectAttributes",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key))
	start := time.Now()
	result, err := t.backend.GetObjectAttributes(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	attrs := []attribute.KeyValue{}
	if input.Bucket != nil {
		attrs = append(attrs, attribute.String("s3.bucket", *input.Bucket))
	}
	if input.Key != nil {
		attrs = append(attrs, attribute.String("s3.key", *input.Key))
	}
	if input.CopySource != nil {
		attrs = append(attrs, attribute.String("s3.copy_source", *input.CopySource))
	}
	ctx, span := t.startSpan(ctx, "CopyObject", attrs...)
	start := time.Now()
	result, err := t.backend.CopyObject(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	ctx, span := t.startSpan(ctx, "ListObjects",
		attribute.String("s3.bucket", *input.Bucket))
	start := time.Now()
	result, err := t.backend.ListObjects(ctx, input)
	if err == nil {
		span.SetAttributes(attribute.Int("s3.object_count", len(result.Contents)))
	}
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	ctx, span := t.startSpan(ctx, "ListObjectsV2",
		attribute.String("s3.bucket", *input.Bucket))
	start := time.Now()
	result, err := t.backend.ListObjectsV2(ctx, input)
	if err == nil {
		span.SetAttributes(attribute.Int("s3.object_count", len(result.Contents)))
	}
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	ctx, span := t.startSpan(ctx, "DeleteObject",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key))
	start := time.Now()
	result, err := t.backend.DeleteObject(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	objectCount := 0
	if input.Delete != nil {
		objectCount = len(input.Delete.Objects)
	}
	ctx, span := t.startSpan(ctx, "DeleteObjects",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.Int("s3.object_count", objectCount))
	start := time.Now()
	result, err := t.backend.DeleteObjects(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) PutObjectAcl(ctx context.Context, input *s3.PutObjectAclInput) error {
	ctx, span := t.startSpan(ctx, "PutObjectAcl",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key))
	start := time.Now()
	err := t.backend.PutObjectAcl(ctx, input)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	ctx, span := t.startSpan(ctx, "ListObjectVersions",
		attribute.String("s3.bucket", *input.Bucket))
	start := time.Now()
	result, err := t.backend.ListObjectVersions(ctx, input)
	t.endSpan(span, start, err)
	return result, err
}

// Special Object Operations

func (t *TracingBackend) RestoreObject(ctx context.Context, input *s3.RestoreObjectInput) error {
	ctx, span := t.startSpan(ctx, "RestoreObject",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key))
	start := time.Now()
	err := t.backend.RestoreObject(ctx, input)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) SelectObjectContent(ctx context.Context, input *s3.SelectObjectContentInput) func(w *bufio.Writer) {
	_, span := t.startSpan(ctx, "SelectObjectContent",
		attribute.String("s3.bucket", *input.Bucket),
		attribute.String("s3.key", *input.Key))
	start := time.Now()
	result := t.backend.SelectObjectContent(ctx, input)
	t.endSpan(span, start, nil)
	return result
}

// Bucket Tagging Operations

func (t *TracingBackend) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	ctx, span := t.startSpan(ctx, "GetBucketTagging",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	result, err := t.backend.GetBucketTagging(ctx, bucket)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) PutBucketTagging(ctx context.Context, bucket string, tags map[string]string) error {
	ctx, span := t.startSpan(ctx, "PutBucketTagging",
		attribute.String("s3.bucket", bucket),
		attribute.Int("s3.tag_count", len(tags)))
	start := time.Now()
	err := t.backend.PutBucketTagging(ctx, bucket, tags)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) DeleteBucketTagging(ctx context.Context, bucket string) error {
	ctx, span := t.startSpan(ctx, "DeleteBucketTagging",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.DeleteBucketTagging(ctx, bucket)
	t.endSpan(span, start, err)
	return err
}

// Object Tagging Operations

func (t *TracingBackend) GetObjectTagging(ctx context.Context, bucket, object, versionId string) (map[string]string, error) {
	ctx, span := t.startSpan(ctx, "GetObjectTagging",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.key", object))
	start := time.Now()
	result, err := t.backend.GetObjectTagging(ctx, bucket, object, versionId)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) PutObjectTagging(ctx context.Context, bucket, object, versionId string, tags map[string]string) error {
	ctx, span := t.startSpan(ctx, "PutObjectTagging",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.key", object),
		attribute.Int("s3.tag_count", len(tags)))
	start := time.Now()
	err := t.backend.PutObjectTagging(ctx, bucket, object, versionId, tags)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) DeleteObjectTagging(ctx context.Context, bucket, object, versionId string) error {
	ctx, span := t.startSpan(ctx, "DeleteObjectTagging",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.key", object))
	start := time.Now()
	err := t.backend.DeleteObjectTagging(ctx, bucket, object, versionId)
	t.endSpan(span, start, err)
	return err
}

// Object Lock Operations

func (t *TracingBackend) PutObjectLockConfiguration(ctx context.Context, bucket string, config []byte) error {
	ctx, span := t.startSpan(ctx, "PutObjectLockConfiguration",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	err := t.backend.PutObjectLockConfiguration(ctx, bucket, config)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	ctx, span := t.startSpan(ctx, "GetObjectLockConfiguration",
		attribute.String("s3.bucket", bucket))
	start := time.Now()
	result, err := t.backend.GetObjectLockConfiguration(ctx, bucket)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) PutObjectRetention(ctx context.Context, bucket, object, versionId string, retention []byte) error {
	ctx, span := t.startSpan(ctx, "PutObjectRetention",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.key", object))
	start := time.Now()
	err := t.backend.PutObjectRetention(ctx, bucket, object, versionId, retention)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) GetObjectRetention(ctx context.Context, bucket, object, versionId string) ([]byte, error) {
	ctx, span := t.startSpan(ctx, "GetObjectRetention",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.key", object))
	start := time.Now()
	result, err := t.backend.GetObjectRetention(ctx, bucket, object, versionId)
	t.endSpan(span, start, err)
	return result, err
}

func (t *TracingBackend) PutObjectLegalHold(ctx context.Context, bucket, object, versionId string, status bool) error {
	ctx, span := t.startSpan(ctx, "PutObjectLegalHold",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.key", object),
		attribute.Bool("s3.legal_hold", status))
	start := time.Now()
	err := t.backend.PutObjectLegalHold(ctx, bucket, object, versionId, status)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) GetObjectLegalHold(ctx context.Context, bucket, object, versionId string) (*bool, error) {
	ctx, span := t.startSpan(ctx, "GetObjectLegalHold",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.key", object))
	start := time.Now()
	result, err := t.backend.GetObjectLegalHold(ctx, bucket, object, versionId)
	t.endSpan(span, start, err)
	return result, err
}

// Non-AWS Operations

func (t *TracingBackend) ChangeBucketOwner(ctx context.Context, bucket, owner string) error {
	ctx, span := t.startSpan(ctx, "ChangeBucketOwner",
		attribute.String("s3.bucket", bucket),
		attribute.String("s3.new_owner", owner))
	start := time.Now()
	err := t.backend.ChangeBucketOwner(ctx, bucket, owner)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingBackend) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	ctx, span := t.startSpan(ctx, "ListBucketsAndOwners")
	start := time.Now()
	result, err := t.backend.ListBucketsAndOwners(ctx)
	t.endSpan(span, start, err)
	return result, err
}
