// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {
  SEMATTRS_DB_STATEMENT,
  SEMATTRS_DB_SYSTEM,
  SEMATTRS_DB_SQL_TABLE,
} from '@opentelemetry/semantic-conventions';

const {TracerProvider} = require('@opentelemetry/sdk-trace-node');

// Optional instrumentation that the user will configure if they'd like to.
const {
  Instrumentation,
  registerInstrumentations,
} = require('@opentelemetry/instrumentation');

import {
  ContextManager,
  Span,
  SpanStatusCode,
  context,
  trace,
  INVALID_SPAN_CONTEXT,
  SpanAttributes,
  TimeInput,
  Link,
  Exception,
  SpanContext,
  SpanStatus,
  SpanKind,
} from '@opentelemetry/api';

export type Span_ = Span;

let optedInPII: boolean = process.env.SPANNER_NODEJS_ANNOTATE_PII_SQL === '1';

interface SQLStatement {
  sql: string;
}

/*
-------------------------------------------------------
Notes and requests from peer review:
-------------------------------------------------------
* TODO: Allow the TracerProvider to be explicitly
    added to receive Cloud Spanner traces.
* TODO: Overkill to instrument all the sessionPool
        methods and avoid adding too many, use discretion.
* TODO: Read Java Spanner to find the nodeTracerProvider
    and find out how they inject it locally or use it globally
    please see https://github.com/googleapis/java-spanner?tab=readme-ov-file#opentelemetry-configuration.
*/

let defaultTracerProvider: typeof TracerProvider = undefined;

// setTracerProvider allows the caller to hook up an OpenTelemetry
// TracerProvider that spans shall be attached to, instead of using
// the global configuration. Later on if `getTracer` is invoked and
// the default tracerProvider is unset, it'll use the global tracer
// otherwise it'll use the set TracerProvider.
export function setTracerProvider(freshTracerProvider: typeof TracerProvider) {
  defaultTracerProvider = freshTracerProvider;
}

// getTracer fetches the tracer each time that it is invoked to solve
// the problem of observability being initialized after Spanner objects
// have been already created.
export function getTracer() {
  if (defaultTracerProvider) {
    return defaultTracerProvider.getTracer('nodejs-spanner');
  }
  // Otherwise use the global tracer still named 'nodejs-spanner'
  return trace.getTracer('nodejs-spanner');
}

export function optInForSQLStatementOnSpans() {
  optedInPII = true;
}

export function optOutOfSQLStatementOnSpans() {
  optedInPII = false;
}

// startTrace synchronously returns a span to avoid the dramatic
// scope change in which trying to use tracer.startActiveSpan
// would change the meaning of this, and also introduction of callbacks
// would radically change all the code structures making it more invasive.
export function startTrace(
  spanNameSuffix: string,
  sql?: string | SQLStatement,
  tableName?: string
): Span {
  const span = getTracer().startSpan(
    'cloud.google.com/nodejs/spanner/' + spanNameSuffix,
    {kind: SpanKind.CLIENT}
  );

  span.setAttribute(SEMATTRS_DB_SYSTEM, 'google.cloud.spanner');

  if (tableName) {
    span.setAttribute(SEMATTRS_DB_SQL_TABLE, tableName);
  }

  if (optedInPII && sql) {
    if (typeof sql === 'string') {
      span.setAttribute(SEMATTRS_DB_STATEMENT, sql as string);
    } else {
      const stmt = sql as SQLStatement;
      span.setAttribute(SEMATTRS_DB_STATEMENT, stmt.sql);
    }
  }

  // Now set the span as the active one in the current context so that
  // future invocations to startTrace will have this current span as
  // the parent.
  trace.setSpan(context.active(), span);
  return span;
}

export function setSpanError(span: Span, err: Error | String) {
  if (!err || !span) {
    return;
  }

  span.setStatus({
    code: SpanStatusCode.ERROR,
    message: err.toString(),
  });
}

export function setGlobalContextManager(manager: ContextManager) {
  context.setGlobalContextManager(manager);
}

export function disableContextAndManager(manager: typeof ContextManager) {
  manager.disable();
  context.disable();
}

// getActiveOrNoopSpan queries the tracer for the currently active span
// and returns it, otherwise if there is no active span available, it'll
// simply create a NoopSpan. This is important in the cases where we don't
// want to create a new span such is sensitive and frequently called code
// for which the new spans would be too many and thus pollute the trace,
// but yet we'd like to record an important annotation.
export function getActiveOrNoopSpan(): Span {
  const span = trace.getActiveSpan();
  if (span) {
    return span;
  }
  return new noopSpan();
}

class noopSpan implements Span {
  constructor() {}

  spanContext(): SpanContext {
    return INVALID_SPAN_CONTEXT;
  }

  setAttribute(key: string, value: unknown): this {
    return this;
  }

  setAttributes(attributes: SpanAttributes): this {
    return this;
  }

  addEvent(name: string, attributes?: SpanAttributes): this {
    return this;
  }

  addLink(link: Link): this {
    return this;
  }

  addLinks(links: Link[]): this {
    return this;
  }

  setStatus(status: SpanStatus): this {
    return this;
  }

  end(endTime?: TimeInput): void {}

  isRecording(): boolean {
    return false;
  }

  recordException(exc: Exception, timeAt?: TimeInput): void {}

  updateName(name: string): this {
    return this;
  }
}
