#!/usr/bin/env node

import { program } from 'commander';
import fs from 'fs';
import path from 'path';

// const version = require('../package.json').version;
const pkg = JSON.parse(fs.readFileSync(
  path.resolve(import.meta.url.replace('file://', ''), '../../../package.json'), 'utf-8'
));

program
  .name('casita')
  .version(pkg.version)
  .usage("<subcmd> [options]")
  .command('image', 'image manipulation')
  .command('block-ring-buffer', 'block ring buffer actions')
  .command('generic', 'non-image data parsing actions')

program.parse(process.argv);