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

import opentelemetry, {SpanStatusCode} from '@opentelemetry/api';

// Ensure that we've registered the gRPC instrumentation.
const {GrpcInstrumentation} = require('@opentelemetry/instrumentation-grpc');
const {BatchSpanProcessor} = require('@opentelemetry/sdk-trace-base');
const {NodeTracerProvider} = require('@opentelemetry/sdk-trace-node');
const {registerInstrumentations} = require('@opentelemetry/instrumentation');

// TODO: Infer the tracer from either the provided context or globally.
const tracer = opentelemetry.trace.getTracer('nodejs-spanner', 'v1.0.0');
const SPAN_CODE_ERROR = SpanStatusCode.ERROR;

export {SPAN_CODE_ERROR, tracer};
console.log('instrument');

export function spanCode(span, err) {
  if (!err) {
    return;
  }

  // References:
  // gRPC status codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
  // OpenTelemetry status codes: https://opentelemetry.io/docs/specs/semconv/rpc/grpc/
  // TODO: File a bug with OpenTelemetry and ask
  // them about the lack of diversity in SpanStatusCode which
  // cannot map to gRPC status codes.
  // const code = err.code? : SPAN_CODE_ERROR;
  // _ = code;
  return;
}

export function startTraceExport(exporter) {
  const provider = new NodeTracerProvider();
  provider.addSpanProcessor(new BatchSpanProcessor(exporter));
  provider.register();

  registerInstrumentations({
    instrumentations: [new GrpcInstrumentation()],
  });
}
