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
const {
  CallbackMethod,
  Class,
  CallbackifyAllOptions,
} = require('@google-cloud/promisify');

import {trace} from '@opentelemetry/api';
// TODO: Infer the tracer from either the provided context or globally.
const tracer = trace.getTracer('nodejs-spanner');
const SPAN_CODE_ERROR = SpanStatusCode.ERROR;

export {SPAN_CODE_ERROR, tracer};

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

/**
 * Wraps a promisy type function to conditionally call a callback function.
 *
 * @param {function} originalMethod - The method to callbackify.
 * @param {object=} options - Callback options.
 * @param {boolean} options.singular - Pass to the callback a single arg instead of an array.
 * @return {function} wrapped
 */
function callbackify(originalMethod: typeof CallbackMethod) {
  if (originalMethod.callbackified_) {
    return originalMethod;
  }

  // tslint:disable-next-line:no-any
  const wrapper = function (this: any) {
    if (typeof arguments[arguments.length - 1] !== 'function') {
      return originalMethod.apply(this, arguments);
    }

    const cb = Array.prototype.pop.call(arguments);

    console.log('cb.name', cb.name);

    tracer.startActiveSpan(
      'cloud.google.com/nodejs/Spanner' + cb.name,
      span => {
        originalMethod.apply(this, arguments).then(
          // tslint:disable-next-line:no-any
          (res: any) => {
            res = Array.isArray(res) ? res : [res];
            span.end();
            cb(null, ...res);
          },
          (err: Error) => {
            span.setStatus({
              code: SPAN_CODE_ERROR,
              message: err.toString(),
            });
            span.end();
            cb(err);
          }
        );
      }
    );
  };
  wrapper.callbackified_ = true;
  return wrapper;
}

/**
 * Callbackifies certain Class methods. This will not callbackify private or
 * streaming methods.
 *
 * @param {module:common/service} Class - Service class.
 * @param {object=} options - Configuration object.
 */
export function callbackifyAll(
  // tslint:disable-next-line:variable-name
  Class: Function,
  options?: typeof CallbackifyAllOptions
) {
  const exclude = (options && options.exclude) || [];
  const ownPropertyNames = Object.getOwnPropertyNames(Class.prototype);
  const methods = ownPropertyNames.filter(methodName => {
    // clang-format off
    return (
      !exclude.includes(methodName) &&
      typeof Class.prototype[methodName] === 'function' && // is it a function?
      !/^_|(Stream|_)|^constructor$/.test(methodName) // is it callbackifyable?
    );
    // clang-format on
  });

  methods.forEach(methodName => {
    const originalMethod = Class.prototype[methodName];
    if (!originalMethod.callbackified_) {
      Class.prototype[methodName] = callbackify(originalMethod);
    }
  });
}
