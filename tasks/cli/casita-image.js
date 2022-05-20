const { program, Option } = require('commander');
const globalOpts = require('./global-opts');
const action = require('../action');

program
  .command('jp2-to-png')
  .requiredOption('-d, --directory <directory>', 'root fragement directory containing fragment-metadata.json file')
  .description('convert jp2 fragement dir to png file')
  .action(action)

program
  .command('composite')
  .addOption(new Option('-d, --directory <directory>', 'directory with fragments folder and fragment-metadata.json file').conflicts('file'))
  .addOption(new Option('-f, --file <files...>', 'fragment files, must be provided in order').conflicts('directory'))
  .description('composite multiple png files together')
  .action(action)

globalOpts(program)

program.parse(process.argv);