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



globalOpts(program)

program.parse(process.argv);