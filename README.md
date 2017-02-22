# Incremental FDs

[![Build Status](https://travis-ci.com/torbenm/incremental-fds.svg?token=mxmHNpq8YYiojbw5SogU&branch=master)](https://travis-ci.com/torbenm/incremental-fds)


## Postgres

Tests are being run with embedded postgres and do not need any further configuration.

For regular database access (e.g. demos), a test database can be set up:

- Make sure postgres is installed
- `psql -f countries.sql -U <user_name> -W -d <db_name>`
	* <db_name> has to be an existing Postgres database (either create one or use "postgres", which already exists by default)
	* <user_name> is your postgres username ("postgres") by default
- create a copy of "properties.default.xml" (in "../database/src/main/resources") named "properties.xml" and adjust the parameters as necessary


## Code Cleanup

Cleaned repositories:

* processor
* 

Documentation still needed: 
* `processor.batch.source.*`