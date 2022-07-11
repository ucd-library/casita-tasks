CaSITA Task Definitions

This folder contains the registered tasks for CaSITA.  CaSITA tasks
should all be integrated in the CLI.

# Adding a task

## 1. Create the NodeJS file to run the task

First, create a new folder or add a file to an existing folder in `tasks/nodejs`.  This folder / file path combination will be referenced in the command line.  Ex: The `casita block-ring-buffer insert` code is located at `tasks/nodejs/block-ring-buffer/insert.js`.  You can alias folders names as well. Ex: `node-image-utils` is aliased to `image` in `node-commons/config.js` so the cli command is `casita image jp2-to-png`.

## 2. Register the cli command

In the `tasks/cli` folder, add to or register an new CLI definition file.  CLI defintion files are broken apart by sub command name.  so `casita.js` runs the main `casita` command and `casita-image.js` handles all `casita image` sub commands.  If you are extending an existing sub command, simply add to the .js file with the commander CLI definition for the command.  Otherwise you will need to edit the `casita.js` file adding the new CLI sub command as well as defining the sub command.  See existing files for reference.

## 3. Develop and test the cli command

Now you should be able to `kubectl exec --stdin --tty` on to a casita worker pod and develop out your new task.

Make sure the return on the main function for your CLI returns ALL metadata that should be included in the kafka message.

## 4. Register the task with argonaut

You can add the task to the Argonaut DAG by editing the `services/nodejs/a6t-controller/lib/graph.js` file.  Follow the Argonaut documentation and use to provided `kafka.js` sink helper the exec your cli command via `kafkaWorker.exec()`.

## 5. Define the tasks kafka topic

This should be done in `config.js` -> kafka -> topics as well as `services/init/kafka.js`.  Make sure your argonaut registration (step 4 above), calls the cli's `-k [topic-name]` flag, so the cli produces kafka messages when completed.  The message will include all data from the return statment of the commands main function.

## 5. Define custom init 

If your task requires init of postgres, you can defined your schema in `services/init/postgres`.

