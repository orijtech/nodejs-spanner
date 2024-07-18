// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

const assert = require('assert');

const {
  AlwaysOffSampler,
  AlwaysOnSampler,
  NodeTracerProvider,
  InMemorySpanExporter,
} = require('@opentelemetry/sdk-trace-node');
const {SimpleSpanProcessor} = require('@opentelemetry/sdk-trace-base');
const {NodeSDK} = require('@opentelemetry/sdk-node');
const {
  addAutoInstrumentation,
  disableContextAndManager,
  setGlobalContextManager,
  startTrace,
} = require('../src/instrument');
const {SEMATTRS_DB_SYSTEM} = require('@opentelemetry/semantic-conventions');

const {ContextManager} = require('@opentelemetry/api');
const {
  AsyncHooksContextManager,
} = require('@opentelemetry/context-async-hooks');
// import {grpc} from 'google-gax';

const projectId = process.env.SPANNER_TEST_PROJECTID || 'orijtech';

describe('Testing spans produced', () => {
  const exporter = new InMemorySpanExporter();
  const sampler = new AlwaysOnSampler();

  let provider: typeof NodeTracerProvider;
  let contextManager: typeof ContextManager;
  let sdk: typeof NodeSDK;

  beforeEach(() => {
    sdk = new NodeSDK({
      traceExporter: exporter,
      sampler: sampler,
    });
    sdk.start();

    provider = new NodeTracerProvider({
      sampler: new AlwaysOnSampler(),
      exporter: exporter,
    });
    provider.addSpanProcessor(new SimpleSpanProcessor(exporter));
    provider.register();

    contextManager = new AsyncHooksContextManager();
    setGlobalContextManager(contextManager);
  });

  afterEach(() => {
    disableContextAndManager(contextManager);
    exporter.reset();
  });

  it('Invoking database methods creates spans: no gRPC tracing enabled yet', async () => {
    const {Spanner} = require('../src');
    const spanner = new Spanner({
      projectId: projectId,
    });
    const instance = spanner.instance('test-instance');
    const databaseAdminClient = spanner.getDatabaseAdminClient();

    const database = instance.database('test-db');

    const query = {sql: 'SELECT 1'};
    const [rows] = await database.run(query);
    assert.strictEqual(rows.length, 1);

    await new Promise((resolve, reject) => setTimeout(resolve, 7850));
    await exporter.forceFlush();

    const spans = exporter.getFinishedSpans();
    // We need to ensure that spans were generated and exported
    // correctly.
    assert.ok(spans.length > 0, 'at least 1 span must have been created');

    // Now sort the spans by duration, in reverse magnitude order.
    spans.sort((spanA, spanB) => {
      return spanA.duration > spanB.duration;
    });

    const got: string[] = [];
    spans.forEach(span => {
      got.push(span.name);
    });

    const want = [
      'cloud.google.com/nodejs/spanner/Spanner.prepareGapicRequest',
      'cloud.google.com/nodejs/spanner/Database.batchCreateSessions',
      'cloud.google.com/nodejs/spanner/Spanner.batchCreateSessions',
      'cloud.google.com/nodejs/spanner/SessionPool.createSessions',
      'cloud.google.com/nodejs/spanner/SessionPool.borrowFromInventory',
      'cloud.google.com/nodejs/spanner/SessionPool.getSession',
      'cloud.google.com/nodejs/spanner/SessionPool.prepareTransaction',
      'cloud.google.com/nodejs/spanner/SessionPool.acquireSession',
      'cloud.google.com/nodejs/spanner/Database.runStream',
      'cloud.google.com/nodejs/spanner/Database.run',
      'cloud.google.com/nodejs/spanner/Spanner.run',
      'cloud.google.com/nodejs/spanner/Transaction.runStream',
      'cloud.google.com/nodejs/spanner/SessionPool.borrowFromInventory',
      'cloud.google.com/nodejs/spanner/SessionPool.getSession',
      'cloud.google.com/nodejs/spanner/SessionPool.prepareTransaction',
      'cloud.google.com/nodejs/spanner/SessionPool.acquireSession',
      'cloud.google.com/nodejs/spanner/Database.runStream',
      'cloud.google.com/nodejs/spanner/Database.run',
      'cloud.google.com/nodejs/spanner/Spanner.run',
      'cloud.google.com/nodejs/spanner/Transaction.runStream',
    ];

    assert.deepEqual(
      want,
      got,
      'The spans order by duration has been violated:\n\tGot:  ' +
        got.toString() +
        '\n\tWant: ' +
        want.toString()
    );

    // Ensure that each span has the attribute
    //  SEMATTRS_DB_SYSTEM, set to 'google.cloud.spanner'
    spans.forEach(span => {
      assert.equal(
        span.attributes[SEMATTRS_DB_SYSTEM],
        'google.cloud.spanner',
        'Missing DB_SYSTEM attribute'
      );
    });
  });

  it('Invoking database methods creates spans: gRPC enabled', async () => {
    addAutoInstrumentation({grpc: true, tracerProvider: provider});

    const {Spanner} = require('../src');
    const spanner = new Spanner({
      projectId: projectId,
    });
    const instance = spanner.instance('test-instance');
    const databaseAdminClient = spanner.getDatabaseAdminClient();

    const database = instance.database('test-db');

    const query = {sql: 'SELECT 1'};
    const [rows] = await database.run(query);
    assert.strictEqual(rows.length, 1);

    await new Promise((resolve, reject) => setTimeout(resolve, 7850));
    await exporter.forceFlush();

    const spans = exporter.getFinishedSpans();
    // We need to ensure that spans were generated and exported
    // correctly.
    assert.ok(spans.length > 0, 'at least 1 span must have been created');

    // Now sort the spans by duration, in reverse magnitude order.
    spans.sort((spanA, spanB) => {
      return spanA.duration > spanB.duration;
    });

    const got: string[] = [];
    spans.forEach(span => {
      got.push(span.name);
    });

    const want = [
      'cloud.google.com/nodejs/spanner/Spanner.prepareGapicRequest',
      'cloud.google.com/nodejs/spanner/Database.batchCreateSessions',
      'cloud.google.com/nodejs/spanner/Spanner.batchCreateSessions',
      'cloud.google.com/nodejs/spanner/SessionPool.createSessions',
      'cloud.google.com/nodejs/spanner/SessionPool.borrowFromInventory',
      'cloud.google.com/nodejs/spanner/SessionPool.getSession',
      'cloud.google.com/nodejs/spanner/SessionPool.prepareTransaction',
      'cloud.google.com/nodejs/spanner/SessionPool.acquireSession',
      'cloud.google.com/nodejs/spanner/Database.runStream',
      'cloud.google.com/nodejs/spanner/Database.run',
      'cloud.google.com/nodejs/spanner/Spanner.run',
      'cloud.google.com/nodejs/spanner/Transaction.runStream',
      'cloud.google.com/nodejs/spanner/SessionPool.borrowFromInventory',
      'cloud.google.com/nodejs/spanner/SessionPool.getSession',
      'cloud.google.com/nodejs/spanner/SessionPool.prepareTransaction',
      'cloud.google.com/nodejs/spanner/SessionPool.acquireSession',
      'cloud.google.com/nodejs/spanner/Database.runStream',
      'cloud.google.com/nodejs/spanner/Database.run',
      'cloud.google.com/nodejs/spanner/Spanner.run',
      'cloud.google.com/nodejs/spanner/Transaction.runStream',
    ];

    assert.deepEqual(
      want,
      got,
      'The spans order by duration has been violated:\n\tGot:  ' +
        got.toString() +
        '\n\tWant: ' +
        want.toString()
    );

    // Ensure that each span has the attribute
    //  SEMATTRS_DB_SYSTEM, set to 'google.cloud.spanner'
    spans.forEach(span => {
      assert.equal(
        span.attributes[SEMATTRS_DB_SYSTEM],
        'google.cloud.spanner',
        'Missing DB_SYSTEM attribute'
      );
    });
  });
  it('Using SessionPool.ping does not create any new spans', () => {});

  it('Closing the client creates spans', () => {});

  it('Turning off the trace sampler does not produce any spans', () => {});

  it('Turning on PII scrubbing does not annotate spans with SQL', () => {});

  it('Turning off PII scrubbing annotates spans with SQL', () => {});
});
