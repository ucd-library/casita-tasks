import { program } from 'commander';
import globalOpts from './global-opts.js';
import action from './action.js';

program
  .command('parse-lightning')
  .requiredOption('-f, --file <file>', 'lightning payload.bin file to parse')
  .description('parse a lightning payload.bin file and generate a payload.json file')
  .action(action)

globalOpts(program)

program.parse(process.argv);