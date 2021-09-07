# transform-ingress2event-time <!-- omit in toc -->
- [Introduction](#introduction)
- [Configuration](#configuration)
  - [Logging](#logging)
- [Development](#development)
  - [Running locally](#running-locally)
  - [tox](#tox)
  - [Commands](#commands)
    - [Linting](#linting)
    - [Tests](#tests)
  
## Introduction
This transformation takes data placed in an "ingress" dataset in regard to their ingested time and moves them to
an "egress" dataset with respect to their event time. The data is aggregated such that data is collected into a single
file based on their date (timestamp excluded).

The transformation takes data from the current time and the previous time and runs the transformation on this collection. 
You can change the current ingesting time by providing a datetime as an argument to the transformation. Then the 
transformation will process data from that hour and the previous hour.

### Usage
```sh
$ python -m transform_ingress2event_time.transform [--ingresss_time %Y-%m-%dT%H']
```


## Configuration

The application needs a configuration file `conf.ini` (see `conf.example.ini`) and a credentials file `credentials.ini`
(see `credentials.example.ini`). The configuration file must 
be placed in the root of the project or in the locations `/etc/osiris/conf.ini` or 
`/etc/transform-ingress2event-time-conf.ini`. 

```
[Logging]
configuration_file = log.conf

[Azure Storage]
account_url = https://<storage_account>.dfs.core.windows.net
filesystem_name = <container_name>

[Datasets]
source = <source_guid>
destination = <destination_guid>
date_format = %%Y-%%m-%%dT%%H:%%M:%%S.%%fZ
date_key_name = <field_name>
time_resolution = <time_resolution: YEAR, MONTH, DAY, HOUR, MINUTE>

[Pipeline]
max_files = 10
parquet_execution = true

[Prometheus]
hostname = <localhost:9091>
environment = <dev/test/prod>
name = <name_of_transformation>
```

The credentials file must be placed in the root of the project or in the
location `/vault/secrets/credentials.ini`

```
[Authorization]
tenant_id = <tenant_id>
client_id = <client_id>
client_secret = <client_secret>
```

### Logging
Logging can be controlled by defining handlers and formatters using [Logging Configuration](https://docs.python.org/3/library/logging.config.html) and specifically the [config fileformat](https://docs.python.org/3/library/logging.config.html#logging-config-fileformat). 
The location of the log configuration file (`Logging.configuration_file`) must be defined in the configuration file of the application as mentioned above.

Here is an example configuration:
```
[loggers]
keys=root

[handlers]
keys=consoleHandler,fileHandler

[formatters]
keys=fileFormatter,consoleFormatter

[logger_root]
level=ERROR
handlers=consoleHandler

[handler_consoleHandler]
class=StreamHandler
formatter=consoleFormatter
args=(sys.stdout,)

[handler_fileHandler]
class=FileHandler
formatter=fileFormatter
args=('logfile.log',)

[formatter_fileFormatter]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s
datefmt=

[formatter_consoleFormatter]
format=%(levelname)s: %(name)s - %(message)s
```

#### Grant access to the dataset
The application must be granted read access to the ingress dataset and write-access to the egress dataset on 
[the Data Platform](https://dataplatform.energinet.dk/).

## Development

### Running locally

### tox

Development for this project relies on [tox](https://tox.readthedocs.io/).

Make sure to have it installed.

### Commands

If you want to run all commands in tox.ini

```sh
$ tox
```

#### Linting

You can also run a single linter specified in tox.ini. For example:

```sh
$ tox -e flake8
```


#### Tests

(No test at the moment - but should be added)

Run unit tests.

```sh
$ tox -e py3
```

Run a specific testcase.

```sh
$ tox -e py3 -- -x tests/test_main.py
```
