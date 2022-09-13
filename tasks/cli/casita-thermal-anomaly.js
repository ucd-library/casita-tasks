import { program, Option } from 'commander';
import globalOpts from './global-opts.js';
import action from './action.js';

program
  .command('detection')
  .requiredOption('-i, --id <roi_buffer_id>', 'roi_buffer_id to run stats on')
  .option('-c, --classifier <classifier>', 'classifier value')
  .description('find pixels above classifier')
  .action(action)

globalOpts(program)

program.parse(process.argv);