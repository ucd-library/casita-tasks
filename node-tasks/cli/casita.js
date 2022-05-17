#!/usr/bin/env node

const { program } = require('commander');
const version = require('../package.json').version;

program
  .name('casita')
  .version(version)
  .usage("<subcmd> [options]")
  .command('image', 'image manipulation')

program.parse(process.argv);