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

'use strict';

function exportSpans(instanceId, databaseId, projectId) {
  // [START spanner_export_traces]
  // Imports the Google Cloud client library and OpenTelemetry libraries for exporting traces.
  // [START setup_tracer]
  const {Resource} = require('@opentelemetry/resources');
  const {NodeSDK} = require('@opentelemetry/sdk-node');
  const {trace} = require('@opentelemetry/api');
  const {
    NodeTracerProvider,
    TraceIdRatioBasedSampler,
  } = require('@opentelemetry/sdk-trace-node');
  const {BatchSpanProcessor} = require('@opentelemetry/sdk-trace-base');
  const {GrpcInstrumentation} = require('@opentelemetry/instrumentation-grpc');
  const {registerInstrumentations} = require('@opentelemetry/instrumentation');
  const {
    TraceExporter,
  } = require('@google-cloud/opentelemetry-cloud-trace-exporter');
  const {
    SEMRESATTRS_SERVICE_NAME,
    SEMRESATTRS_SERVICE_VERSION,
  } = require('@opentelemetry/semantic-conventions');

  const resource = Resource.default().merge(
    new Resource({
      [SEMRESATTRS_SERVICE_NAME]: 'spanner-sample',
      [SEMRESATTRS_SERVICE_VERSION]: 'v1.0.0', // Whatever version of your app is running.,
    })
  );
  const exporter = new TraceExporter();

  const sdk = new NodeSDK({
    resource: resource,
    traceExporter: exporter,
    // Trace every single request to ensure that we generate
    // enough traffic for proper examination of traces.
    sampler: new TraceIdRatioBasedSampler(1.0),
  });
  sdk.start();

  const provider = new NodeTracerProvider({resource: resource});
  provider.addSpanProcessor(new BatchSpanProcessor(exporter));
  provider.register();

  registerInstrumentations({
    instrumentations: [new GrpcInstrumentation()],
  });

  // OpenTelemetry MUST be imported much earlier than the cloud-spanner package.
  const {Spanner} = require('@google-cloud/spanner');
  const tracer = trace.getTracer('nodejs-spanner');
  // [END setup_tracer]

  /**
   * TODO(developer): Uncomment the following lines before running the sample.
   */
  // const projectId = 'my-project-id';
  // const instanceId = 'my-instance';
  // const databaseId = 'my-database';

  // Creates a client
  const spanner = new Spanner({
    projectId: projectId,
  });

  // Gets a reference to a Cloud Spanner instance and database
  const instance = spanner.instance(instanceId);
  const database = instance.database(databaseId);

  // Gets a transaction object that captures the database state
  // at a specific point in time
  tracer.startActiveSpan('gotSnapshot', span => {
    database.getSnapshot(async (err, transaction) => {
      if (err) {
        console.error(err);
        return;
      }
      const queryOne =
        "SELECT * FROM information_schema.tables WHERE table_schema = ''";

      let i = 0;
      for (i = 0; i < 1; i++) {
        try {
          // Read #1, using SQL
          const [qOneRows] = await transaction.run(queryOne);

          qOneRows.forEach(row => {
            const json = JSON.stringify(row.toJSON());
            console.log(`Catalog: ${json}`);
          });
          console.log('Successfully executed read-only transaction.');
        } catch (err) {
          console.error('ERROR:', err);
        } finally {
          transaction.end();
          // Close the database when finished.
          await database.close();
          console.log('Completed');
        }
      }
    });

    span.end();
    setTimeout(() => console.log('ended'), 30000);
  });
  // [END spanner_export_traces]
}

require('yargs')
  .demand(1)
  .command(
    'export <instanceName> <databaseName> <projectId>',
    'Execute a read-only transaction on an example Cloud Spanner table.',
    {},
    opts => exportSpans(opts.instanceName, opts.databaseName, opts.projectId)
  )
  .example('node $0 readOnly "my-instance" "my-database" "my-project-id"')
  .example('node $0 readWrite "my-instance" "my-database" "my-project-id"')
  .wrap(120)
  .recommendCommands()
  .epilogue('For more information, see https://cloud.google.com/spanner/docs')
  .strict()
  .help().argv;
