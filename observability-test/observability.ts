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
const {
  disableContextAndManager,
  setGlobalContextManager,
  startTrace,
  optInForSQLStatementOnSpans,
  optOutOfSQLStatementOnSpans,
  setTracerProvider,
} = require('../src/instrument');
const {
  SEMATTRS_DB_STATEMENT,
  SEMATTRS_DB_SYSTEM,
} = require('@opentelemetry/semantic-conventions');

const {ContextManager} = require('@opentelemetry/api');
const {
  AsyncHooksContextManager,
} = require('@opentelemetry/context-async-hooks');

const projectId = process.env.SPANNER_TEST_PROJECTID || 'orijtech';

describe('Testing spans produced with a sampler on', () => {
  const exporter = new InMemorySpanExporter();
  const sampler = new AlwaysOnSampler();

  let provider: typeof NodeTracerProvider;
  let contextManager: typeof ContextManager;

  const {Spanner} = require('../src');
  const spanner = new Spanner({
    projectId: projectId,
  });
  const instance = spanner.instance('test-instance');
  const database = instance.database('test-db');

  beforeEach(() => {
    provider = new NodeTracerProvider({
      sampler: sampler,
      exporter: exporter,
    });
    provider.addSpanProcessor(new SimpleSpanProcessor(exporter));
    provider.register();
    setTracerProvider(provider);

    contextManager = new AsyncHooksContextManager();
    setGlobalContextManager(contextManager);
  });

  afterEach(async () => {
    disableContextAndManager(contextManager);
    exporter.forceFlush();
    exporter.reset();
  });

  after(async () => {
    spanner.close();
    await provider.shutdown();
  });

  it('Invoking database methods creates spans: no gRPC instrumentation', async () => {
    const query = {sql: 'SELECT 1'};
    const [rows] = await database.run(query);
    assert.strictEqual(rows.length, 1);

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

  const methodsTakingSQL = {
    'cloud.google.com/nodejs/spanner/Database.run': true,
    'cloud.google.com/nodejs/spanner/Transaction.runStream': true,
  };

  it('Opt-ing into PII-risk SQL annotation on spans works', async () => {
    optInForSQLStatementOnSpans();

    const {Spanner} = require('../src');
    const spanner = new Spanner({
      projectId: projectId,
    });
    const instance = spanner.instance('test-instance');
    const database = instance.database('test-db');

    const query = {sql: 'SELECT CURRENT_TIMESTAMP()'};
    const [rows] = await database.run(query);
    assert.strictEqual(rows.length, 1);

    const spans = exporter.getFinishedSpans();
    // We need to ensure that spans were generated and exported
    // correctly.
    assert.ok(spans.length > 0, 'at least 1 span must have been created');

    // Ensure that each span has the attribute
    //  SEMATTRS_DB_SYSTEM, set to 'google.cloud.spanner'
    spans.forEach(span => {
      if (!methodsTakingSQL[span.name]) {
        return;
      }

      const got = span.attributes[SEMATTRS_DB_STATEMENT];
      const want = query.sql;
      assert.strictEqual(
        got,
        want,
        `${span.name} has Invalid DB_STATEMENT attribute\n\tGot:  ${got}\n\tWant: ${want}`
      );
    });
  });

  it('Closing the client creates the closing span', () => {
    const {Spanner} = require('../src');
    const spanner = new Spanner({
      projectId: projectId,
    });
    spanner.close();

    const spans = exporter.getFinishedSpans();
    // We need to ensure that spans were generated and exported
    // correctly.
    assert.ok(spans.length == 1, 'exactly 1 span must have been created');
    assert.strictEqual(
      spans[0].name,
      'cloud.google.com/nodejs/spanner/Spanner.close'
    );
  });
});

describe('Always off sampler used', () => {
  const exporter = new InMemorySpanExporter();
  const sampler = new AlwaysOffSampler();

  let provider: typeof NodeTracerProvider;
  let contextManager: typeof ContextManager;

  const {Spanner} = require('../src');
  const spanner = new Spanner({
    projectId: projectId,
  });
  const instance = spanner.instance('test-instance');
  const database = instance.database('test-db');

  beforeEach(() => {
    provider = new NodeTracerProvider({
      sampler: sampler,
      exporter: exporter,
    });
    provider.addSpanProcessor(new SimpleSpanProcessor(exporter));
    provider.register();
    setTracerProvider(provider);

    contextManager = new AsyncHooksContextManager();
    setGlobalContextManager(contextManager);
  });

  afterEach(async () => {
    disableContextAndManager(contextManager);
    exporter.forceFlush();
    exporter.reset();
  });

  after(async () => {
    spanner.close();
    await provider.shutdown();
  });

  it('Querying with gRPC enabled', async () => {
    const query = {sql: 'SELECT 1'};
    const [rows] = await database.run(query);
    assert.strictEqual(rows.length, 1);

    const spans = exporter.getFinishedSpans();
    assert.strictEqual(spans.length, 0, 'no spans should be exported');
  });

  it('Opt-ing into PII-risk SQL annotation', async () => {
    optInForSQLStatementOnSpans();

    const query = {sql: 'SELECT CURRENT_TIMESTAMP()'};
    const [rows] = await database.run(query);
    assert.strictEqual(rows.length, 1);

    const spans = exporter.getFinishedSpans();
    assert.strictEqual(spans.length, 0, 'no spans must be created');
  });
});

describe('Enabled gRPC instrumentation with sampling on', () => {
  const {registerInstrumentations} = require('@opentelemetry/instrumentation');
  const {GrpcInstrumentation} = require('@opentelemetry/instrumentation-grpc');

  const exporter = new InMemorySpanExporter();
  const sampler = new AlwaysOnSampler();

  let provider: typeof NodeTracerProvider;
  let contextManager: typeof ContextManager;

  beforeEach(() => {
    registerInstrumentations({
      instrumentations: [new GrpcInstrumentation()],
    });
    provider = new NodeTracerProvider({
      sampler: sampler,
    });
    provider.addSpanProcessor(new SimpleSpanProcessor(exporter));
    provider.register();

    contextManager = new AsyncHooksContextManager();
    setGlobalContextManager(contextManager);
  });

  afterEach(async () => {
    disableContextAndManager(contextManager);
    exporter.forceFlush();
    exporter.reset();
  });

  after(async () => {
    await provider.shutdown();
  });

  it('Invoking database methods creates spans: gRPC enabled', async () => {
    const {Spanner} = require('../src');
    setTracerProvider(provider);
    const spanner = new Spanner({
      projectId: projectId,
    });
    const instance = spanner.instance('test-instance');
    const database = instance.database('test-db');
    const query = {sql: 'SELECT 1'};
    const [rows] = await database.run(query);
    assert.strictEqual(rows.length, 1);
    database.close();
    spanner.close();

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
      'cloud.google.com/nodejs/spanner/Database.batchCreateSessions',
      'cloud.google.com/nodejs/spanner/Spanner.batchCreateSessions',
      'grpc.google.spanner.v1.Spanner/ExecuteStreamingSql',
      'cloud.google.com/nodejs/spanner/Database.runStream',
      'cloud.google.com/nodejs/spanner/Database.run',
      'cloud.google.com/nodejs/spanner/Spanner.run',
      'cloud.google.com/nodejs/spanner/Transaction.runStream',
      'cloud.google.com/nodejs/spanner/Spanner.close',
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
      if (span.name.startsWith('cloud.google.com')) {
        assert.equal(
          span.attributes[SEMATTRS_DB_SYSTEM],
          'google.cloud.spanner',
          'Invalid DB_SYSTEM attribute'
        );
      }
    });
  });
});
