import { program, Option } from 'commander';
import globalOpts from './global-opts.js';
import action from './action.js';

program
  .command('insert')
  .requiredOption('-f, --file <file>', 'file to insert')
  .description('insert file into ring buffer')
  .action(action)

globalOpts(program)

program.parse(process.argv);