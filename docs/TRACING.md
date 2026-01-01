# Distributed Tracing Guide

This guide explains how to enable and use OpenTelemetry (OTEL) distributed tracing in VersityGW.

## Overview

VersityGW includes comprehensive OpenTelemetry tracing that provides visibility into:

- **HTTP Layer**: All incoming S3 API requests
- **Authentication**: Signature verification and account lookups
- **Backend Operations**: All storage backend calls (ListObjects, PutObject, etc.)
- **IAM Operations**: User account management
- **Multipart Uploads**: Detailed tracing of multipart operations

## Enabling Tracing

### CLI Flags

```bash
versitygw --otel-enabled \
  --otel-endpoint "localhost:4317" \
  --otel-service-name "versitygw-prod" \
  --otel-environment "production" \
  --otel-sample-rate 1.0 \
  posix /path/to/storage
```

### Environment Variables

```bash
export VGW_OTEL_ENABLED=true
export VGW_OTEL_ENDPOINT="localhost:4317"
export VGW_OTEL_SERVICE_NAME="versitygw"
export VGW_OTEL_ENVIRONMENT="production"
export VGW_OTEL_SAMPLE_RATE="1.0"

versitygw posix /path/to/storage
```

### Configuration Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--otel-enabled` | `VGW_OTEL_ENABLED` | `false` | Enable OTEL tracing |
| `--otel-endpoint` | `VGW_OTEL_ENDPOINT` | `localhost:4317` | OTLP endpoint |
| `--otel-service-name` | `VGW_OTEL_SERVICE_NAME` | `versitygw` | Service name in traces |
| `--otel-environment` | `VGW_OTEL_ENVIRONMENT` | `development` | Environment tag |
| `--otel-sample-rate` | `VGW_OTEL_SAMPLE_RATE` | `1.0` | Sampling rate (0.0-1.0) |
| `--otel-use-http` | `VGW_OTEL_USE_HTTP` | `false` | Use HTTP instead of gRPC |
| `--otel-insecure` | `VGW_OTEL_INSECURE` | `false` | Disable TLS |

## Trace Structure

### HTTP Request Span (Root)

Every S3 API request creates a root span with:

```
S3.GetObject
├── Attributes:
│   ├── http.method: GET
│   ├── http.url: /bucket/key
│   ├── http.status_code: 200
│   ├── s3.bucket: mybucket
│   ├── s3.key: myobject
│   ├── http.client_ip: 192.168.1.100
│   └── http.user_agent: aws-cli/2.x
├── Children:
│   ├── Auth.V4Signature
│   └── Backend.GetObject
```

### Authentication Span

```
Auth.V4Signature
├── Attributes:
│   ├── auth.access_key: AKIAIOSFODNN7EXAMPLE
│   ├── auth.role: user
│   ├── auth.is_root: false
│   └── duration_ms: 2
```

### Backend Span

```
Backend.GetObject
├── Attributes:
│   ├── backend.type: posix
│   ├── backend.operation: GetObject
│   ├── s3.bucket: mybucket
│   ├── s3.key: myobject
│   ├── s3.content_length: 1048576
│   └── duration_ms: 45
```

### Multipart Upload Trace

```
S3.CompleteMultipartUpload
├── Auth.V4Signature
└── Backend.CompleteMultipartUpload
    ├── Attributes:
    │   ├── s3.upload_id: abc123
    │   ├── s3.part_count: 100
    │   └── duration_ms: 5000
```

### Deep I/O Tracing (Posix Backend)

For the posix backend, tracing goes all the way down to file system operations:

#### PutObject (Single File Upload)
```
S3.PutObject
├── Auth.V4Signature
└── Backend.PutObject
    ├── Posix.IO.OpenTmpFile          # Create temp file with O_TMPFILE
    │   ├── posix.dir: /data/.sgwtmp
    │   ├── posix.bucket: mybucket
    │   ├── posix.object: myfile.bin
    │   └── posix.size: 104857600
    ├── Posix.IO.WriteData            # Stream data to disk
    │   ├── posix.bytes_written: 104857600
    │   ├── posix.duration_ms: 450
    │   └── posix.throughput_mbps: 222.5
    └── Posix.IO.LinkFile             # Atomic rename into namespace
        ├── posix.bucket: mybucket
        └── posix.object: myfile.bin
```

#### UploadPart (Multipart Part Upload)
```
S3.UploadPart
├── Auth.V4Signature
└── Backend.UploadPart
    ├── Posix.IO.OpenTmpFile          # Create temp file for part
    │   └── posix.size: 104857600
    ├── Posix.IO.WriteData            # Write part data to disk
    │   ├── posix.bytes_written: 104857600
    │   └── posix.throughput_mbps: 250.0
    └── Posix.IO.LinkFile             # Link part into upload dir
```

#### CompleteMultipartUpload (Part Assembly)
```
S3.CompleteMultipartUpload
├── Auth.V4Signature
└── Backend.CompleteMultipartUpload
    └── Posix.IO.AssembleParts        # Overall assembly operation
        ├── posix.total_parts: 100
        ├── posix.total_size: 10737418240  # 10GB
        ├── Children (per part):
        │   ├── Posix.IO.CopyPart (part 1)
        │   │   ├── posix.part_number: 1
        │   │   ├── posix.part_size: 104857600
        │   │   ├── posix.copy_method: copy_file_range  # Zero-copy!
        │   │   └── posix.throughput_mbps: 2500.0
        │   │   └── Posix.IO.FastFileCopy
        │   │       └── Posix.IO.CopyFileRange
        │   │           ├── posix.bytes_copied: 104857600
        │   │           └── posix.used_fallback: false
        │   ├── Posix.IO.CopyPart (part 2)
        │   │   └── ... (with checksum, uses io.Copy)
        │   └── ...
        ├── Events:
        │   ├── part_assembled {current_part: 1, bytes_copied: 104857600}
        │   ├── part_assembled {current_part: 2, bytes_copied: 209715200}
        │   └── ...
        └── posix.duration_ms: 4200
```

#### Tracing copy_file_range vs io.Copy

The tracing distinguishes between different copy methods:

| Method | When Used | Performance |
|--------|-----------|-------------|
| `copy_file_range` | Direct file-to-file, no checksum streaming | Zero-copy (kernel) |
| `io_copy_with_checksum` | When checksum must be computed | User-space copy |
| `custom_move` | ScoutFS block move operations | Block-level |
| `io_copy_fallback` | Fallback when custom_move fails | User-space copy |

## Backend Setup

### Jaeger

```bash
# Run Jaeger all-in-one
docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest

# Start VersityGW with tracing
versitygw --otel-enabled --otel-endpoint localhost:4317 --otel-insecure \
  posix /data
```

Access Jaeger UI at http://localhost:16686

### Grafana Tempo

```yaml
# docker-compose.yml
version: '3'
services:
  tempo:
    image: grafana/tempo:latest
    command: [ "-config.file=/etc/tempo.yaml" ]
    volumes:
      - ./tempo.yaml:/etc/tempo.yaml
    ports:
      - "4317:4317"
      - "3200:3200"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_FEATURE_TOGGLES_ENABLE=traceqlEditor
```

```yaml
# tempo.yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317

storage:
  trace:
    backend: local
    local:
      path: /tmp/tempo/blocks
```

### Honeycomb

```bash
versitygw --otel-enabled \
  --otel-endpoint "api.honeycomb.io:443" \
  --otel-service-name "versitygw" \
  posix /data

# Set API key via header (requires custom configuration)
```

### New Relic

```bash
versitygw --otel-enabled \
  --otel-endpoint "otlp.nr-data.net:4317" \
  --otel-service-name "versitygw" \
  posix /data
```

## Sampling Strategies

### Development (100% sampling)
```bash
--otel-sample-rate 1.0
```

### Production (10% sampling)
```bash
--otel-sample-rate 0.1
```

### High-volume (1% sampling)
```bash
--otel-sample-rate 0.01
```

## Trace Context Propagation

VersityGW supports W3C Trace Context propagation. The trace ID is returned in the `X-Trace-ID` response header for debugging:

```bash
$ curl -v http://localhost:7070/mybucket/mykey
< X-Trace-ID: 4bf92f3577b34da6a3ce929d0e0e4736
```

Use this trace ID to search for the request in your tracing backend.

## Custom Attributes

Traces automatically include:

### S3 Attributes
- `s3.bucket` - Bucket name
- `s3.key` - Object key
- `s3.operation` - S3 operation name
- `s3.upload_id` - Multipart upload ID
- `s3.part_number` - Part number
- `s3.content_length` - Content size
- `s3.etag` - Object ETag
- `s3.version_id` - Version ID

### Backend Attributes
- `backend.type` - Backend type (posix, scoutfs, azure, s3proxy)
- `backend.operation` - Backend operation name
- `duration_ms` - Operation duration in milliseconds

### Auth Attributes
- `auth.method` - Authentication method
- `auth.access_key` - Access key (for debugging)
- `auth.role` - User role
- `auth.is_root` - Whether request is from root user

## Performance Impact

Tracing adds minimal overhead:
- ~1-2ms per request for span creation
- Network overhead depends on OTLP endpoint latency
- Use sampling to reduce overhead in high-volume scenarios

## Troubleshooting

### No traces appearing

1. Check OTEL endpoint connectivity:
   ```bash
   nc -zv localhost 4317
   ```

2. Enable debug logging:
   ```bash
   VGW_DEBUG=true versitygw --otel-enabled ...
   ```

3. Try HTTP instead of gRPC:
   ```bash
   --otel-use-http --otel-endpoint localhost:4318
   ```

### Incomplete traces

Check sampling rate - at low rates, some traces may not be captured.

### High latency

1. Reduce sample rate
2. Use async batching (default)
3. Check OTLP endpoint performance

## Integration with Sentry

When both OTEL and Sentry are enabled, they work together:
- OTEL provides detailed traces
- Sentry captures errors with trace context

```bash
versitygw --otel-enabled --otel-endpoint localhost:4317 \
  --sentry-enabled --sentry-dsn "https://..." \
  posix /data
```

Errors in Sentry will include trace IDs for correlation.

## Metrics Integration

Traces can be converted to metrics using your tracing backend:
- Tempo: TraceQL metrics
- Jaeger: SPM (Service Performance Monitoring)
- Honeycomb: Derived columns

Example TraceQL query for p99 latency:
```
{ resource.service.name = "versitygw" } | rate() by (span.s3.operation)
```
