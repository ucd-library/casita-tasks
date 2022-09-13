import { program, Option } from 'commander';
import globalOpts from './global-opts.js';
import action from './action.js';

program
  .command('create')
  .requiredOption('-b, --band <band>', 'band to use')
  .requiredOption('-d, --datetime <datetime>', 'timestamp to create roi on')
  .requiredOption('-p, --product <product>', 'product to create roi on')
  .description('create the california roi')
  .action(action)

  program
  .command('hourly-max-stats')
  .requiredOption('-i, --id <roi_buffer_id>', 'roi_buffer_id to run stats on')
  .description('run hourly stats on given id')
  .action(action)


globalOpts(program)

program.parse(process.argv);