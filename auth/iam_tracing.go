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

package auth

import (
	"time"

	"github.com/versity/versitygw/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TracingIAMService wraps an IAMService with OpenTelemetry tracing
type TracingIAMService struct {
	iam IAMService
}

// NewTracingIAMService creates a new tracing wrapper for IAMService
func NewTracingIAMService(iam IAMService) *TracingIAMService {
	return &TracingIAMService{iam: iam}
}

func (t *TracingIAMService) startSpan(operation string, attrs ...attribute.KeyValue) trace.Span {
	allAttrs := append([]attribute.KeyValue{
		attribute.String("iam.operation", operation),
	}, attrs...)

	_, span := observability.Tracer().Start(nil, "IAM."+operation,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(allAttrs...),
	)
	return span
}

func (t *TracingIAMService) endSpan(span trace.Span, start time.Time, err error) {
	span.SetAttributes(attribute.Int64("duration_ms", time.Since(start).Milliseconds()))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

func (t *TracingIAMService) CreateAccount(account Account) error {
	span := t.startSpan("CreateAccount",
		attribute.String("iam.access_key", account.Access),
		attribute.String("iam.role", string(account.Role)),
	)
	start := time.Now()
	err := t.iam.CreateAccount(account)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingIAMService) GetUserAccount(access string) (Account, error) {
	span := t.startSpan("GetUserAccount",
		attribute.String("iam.access_key", access),
	)
	start := time.Now()
	account, err := t.iam.GetUserAccount(access)
	if err == nil {
		span.SetAttributes(attribute.String("iam.role", string(account.Role)))
	}
	t.endSpan(span, start, err)
	return account, err
}

func (t *TracingIAMService) UpdateUserAccount(access string, props MutableProps) error {
	span := t.startSpan("UpdateUserAccount",
		attribute.String("iam.access_key", access),
	)
	start := time.Now()
	err := t.iam.UpdateUserAccount(access, props)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingIAMService) DeleteUserAccount(access string) error {
	span := t.startSpan("DeleteUserAccount",
		attribute.String("iam.access_key", access),
	)
	start := time.Now()
	err := t.iam.DeleteUserAccount(access)
	t.endSpan(span, start, err)
	return err
}

func (t *TracingIAMService) ListUserAccounts() ([]Account, error) {
	span := t.startSpan("ListUserAccounts")
	start := time.Now()
	accounts, err := t.iam.ListUserAccounts()
	if err == nil {
		span.SetAttributes(attribute.Int("iam.account_count", len(accounts)))
	}
	t.endSpan(span, start, err)
	return accounts, err
}

func (t *TracingIAMService) Shutdown() error {
	span := t.startSpan("Shutdown")
	start := time.Now()
	err := t.iam.Shutdown()
	t.endSpan(span, start, err)
	return err
}
