import { Option } from 'commander';

function wrapOpts(program) {
  program.commands.forEach(cmd => {
    cmd
      .option('-m, --metrics', 'record google cloud metrics')
      .addOption(new Option(
        '--google-application-credentials <file>', 
        'path to google cloud credentials file (required for metrics)'
        )
        .default('/etc/google/service-account.json')
        .env('GOOGLE_APPLICATION_CREDENTIALS')
      )
      .addOption(new Option(
        '--google-project-id <projectId>', 
        'google project id to write metrics (required for metrics)'
        )
        .env('GOOGLE_PROJECT_ID')
      )
      .option('-k, --kafka <topic>', 'send kafka message on complete')
      .option('-e, --kafka-external', 'send kafka message to external *-ext topic as well')
      .addOption(
        new Option('--kafka-port <port>', 'port kafka is running on')
          .default(9092)
          .env('KAFKA_PORT')
      )
      .addOption(
        new Option('--kafka-host <host>', 'hostname kafka for kafka')
          .default('kafka')
          .env('KAFKA_HOST')
      )
      .option('-p, --print-kafka-msg', 'send kafka message to stdout')
      .option('--quiet', 'disabled stdout/stderr')
      .option('--debug-config', 'print config to stdout')
  })
}

export default wrapOpts;