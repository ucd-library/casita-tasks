import { program, Option } from 'commander';
import globalOpts from './global-opts.js';
import action from './action.js';

program
  .command('insert')
  .requiredOption('-f, --file <file>', 'file to insert')
  .description('insert file into ring buffer')
  .action(action)

program
  .command('hourly-max-stats')
  .requiredOption('-i, --id <blocks_ring_buffer_id>', 'blocks_ring_buffer_id to run stats on')
  .description('run hourly stats on given id')
  .action(action)

globalOpts(program)

program.parse(process.argv);