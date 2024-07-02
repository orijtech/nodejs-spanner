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

import opentelemetry, {SpanStatusCode, Tracer} from '@opentelemetry/api';

// TODO: Infer the tracer from either the provided context or globally.
const tracer = opentelemetry.trace.getTracer('nodejs-spanner', 'v1.0.0');
const SPAN_CODE_ERROR = SpanStatusCode.ERROR;

export {SPAN_CODE_ERROR, tracer};
console.log('instrument');
