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
  const {Spanner} = require('@google-cloud/spanner');
  // const {TraceExporter} = require('@google-cloud/opentelemetry-cloud-trace-exporter');
  const {Resource} = require('@opentelemetry/resources');
  const {NodeSDK} = require('@opentelemetry/sdk-node');
  const {WebTracerProvider} = require('@opentelemetry/sdk-trace-web');
  const {
    SEMRESATTRS_SERVICE_NAME,
    SEMRESATTRS_SERVICE_VERSION,
  } = require('@opentelemetry/semantic-conventions');
  const {
    BatchSpanProcessor,
    ConsoleSpanExporter,
  } = require('@opentelemetry/sdk-trace-base');

  const resource = Resource.default().merge(
    new Resource({
      [SEMRESATTRS_SERVICE_NAME]: 'spanner-sample',
      [SEMRESATTRS_SERVICE_VERSION]: 'v1.0.0', // Whatever version of your app is running.,
    })
  );
  const exporter = new ConsoleSpanExporter();
  const sdk = new NodeSDK({
    resource: resource,
    traceExporter: exporter,
  });
  sdk.start();
  const provider = new WebTracerProvider({resource: resource});
  const processor = new BatchSpanProcessor(exporter);
  provider.addSpanProcessor(processor);
  provider.register();

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
  database.getSnapshot(async (err, transaction) => {
    if (err) {
      console.error(err);
      return;
    }
    const queryOne =
      'SELECT * FROM exchange_rates ORDER BY created_at DESC limit 10';

    try {
      // Read #1, using SQL
      const [qOneRows] = await transaction.run(queryOne);

      qOneRows.forEach(row => {
        const json = row.toJSON();
        console.log(
          `Id: ${json.id}, Value: ${json.value}, BaseCurrency: ${json.base_curr}`
        );
      });
      console.log('Successfully executed read-only transaction.');
    } catch (err) {
      console.error('ERROR:', err);
    } finally {
      transaction.end();
      // Close the database when finished.
      await database.close();
    }
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
