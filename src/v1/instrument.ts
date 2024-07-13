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

import {Span, SpanStatusCode} from '@opentelemetry/api';

// Ensure that we've registered the gRPC instrumentation.
const {GrpcInstrumentation} = require('@opentelemetry/instrumentation-grpc');
const {BatchSpanProcessor} = require('@opentelemetry/sdk-trace-base');
const {NodeTracerProvider} = require('@opentelemetry/sdk-trace-node');
const {HttpInstrumentation} = require('@opentelemetry/instrumentation-http');
const {registerInstrumentations} = require('@opentelemetry/instrumentation');
const {
  CallbackMethod,
  Class,
  CallbackifyAllOptions,
  PromiseMethod,
  PromisifyAllOptions,
  PromisifyOptions,
  WithPromise,
} = require('@google-cloud/promisify');

import {trace} from '@opentelemetry/api';
// TODO: Infer the tracer from either the provided context or globally.
const tracer = trace.getTracer('nodejs-spanner');
const SPAN_CODE_ERROR = SpanStatusCode.ERROR;

registerInstrumentations({
  instrumentations: [new GrpcInstrumentation(), new HttpInstrumentation()],
});

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
}

/**
 * Wraps a promisy type function to conditionally call a callback function.
 *
 * @param {function} originalMethod - The method to callbackify.
 * @param {object=} options - Callback options.
 * @param {boolean} options.singular - Pass to the callback a single arg instead of an array.
 * @return {function} wrapped
 *
 * This code although modified for OpenTelemetry instrumentation, is copied from
 * https://github.com/googleapis/nodejs-promisify/blob/main/src/index.ts
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
    const that = this;

    tracer.startActiveSpan(
      'cloud.google.com/nodejs/Spanner.' + cb.name + '.callbackify',
      span => {
        originalMethod.apply(that, arguments).then(
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
 *
 * This code although modified for OpenTelemetry instrumentation, is copied from
 * https://github.com/googleapis/nodejs-promisify/blob/main/src/index.ts
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

/**
 * Wraps a callback style function to conditionally return a promise.
 *
 * @param {function} originalMethod - The method to promisify.
 * @param {object=} options - Promise options.
 * @param {boolean} options.singular - Resolve the promise with single arg instead of an array.
 * @return {function} wrapped
 *
 * This code although modified for OpenTelemetry instrumentation, is copied from
 * https://github.com/googleapis/nodejs-promisify/blob/main/src/index.ts
 */
export function promisify(
  originalMethod: typeof PromiseMethod,
  options?: typeof PromisifyOptions
) {
  if (originalMethod.promisified_) {
    return originalMethod;
  }

  options = options || {};

  const slice = Array.prototype.slice;

  const wrapper: any = function (this: typeof WithPromise) {
    const that = this;

    return tracer.startActiveSpan(
      'cloud.google.com/nodejs/Spanner.' + originalMethod.name + '.promisify',
      span => {
        // tslint:disable-next-line:no-any
        let last;

        for (last = arguments.length - 1; last >= 0; last--) {
          const arg = arguments[last];

          if (typeof arg === 'undefined') {
            continue; // skip trailing undefined.
          }

          if (typeof arg !== 'function') {
            break; // non-callback last argument found.
          }

          return originalMethod.apply(that, arguments);
        }

        // peel trailing undefined.
        const args = slice.call(arguments, 0, last + 1);

        // tslint:disable-next-line:variable-name
        let PromiseCtor = Promise;

        // Because dedupe will likely create a single install of
        // @google-cloud/common to be shared amongst all modules, we need to
        // localize it at the Service level.
        if (that && that.Promise) {
          PromiseCtor = that.Promise;
        }

        return new PromiseCtor((resolve, reject) => {
          // tslint:disable-next-line:no-any
          args.push((...args: any[]) => {
            const callbackArgs = slice.call(args);
            const err = callbackArgs.shift();

            if (err) {
              span.setStatus({
                code: SPAN_CODE_ERROR,
                message: err.toString(),
              });
              span.end();
              return reject(err);
            }

            span.end();
            if (options!.singular && callbackArgs.length === 1) {
              resolve(callbackArgs[0]);
            } else {
              resolve(callbackArgs);
            }
          });

          originalMethod.apply(that, args);
        });
      }
    );
  };

  wrapper.promisified_ = true;
  return wrapper;
}

/**
 * Promisifies certain Class methods. This will not promisify private or
 * streaming methods.
 *
 * @param {module:common/service} Class - Service class.
 * @param {object=} options - Configuration object.
 *
 * This code although modified for OpenTelemetry instrumentation, is copied from
 * https://github.com/googleapis/nodejs-promisify/blob/main/src/index.ts
 */
// tslint:disable-next-line:variable-name
export function promisifyAll(
  Class: Function,
  options?: typeof PromisifyAllOptions
) {
  const exclude = (options && options.exclude) || [];
  const ownPropertyNames = Object.getOwnPropertyNames(Class.prototype);
  const methods = ownPropertyNames.filter(methodName => {
    // clang-format off
    return (
      !exclude.includes(methodName) &&
      typeof Class.prototype[methodName] === 'function' && // is it a function?
      !/(^_|(Stream|_)|promise$)|^constructor$/.test(methodName) // is it promisable?
    );
    // clang-format on
  });

  methods.forEach(methodName => {
    const originalMethod = Class.prototype[methodName];
    if (!originalMethod.promisified_) {
      Class.prototype[methodName] = exports.promisify(originalMethod, options);
    }
  });
}
