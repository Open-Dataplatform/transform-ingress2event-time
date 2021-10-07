"""
Transforms data structured in the filesystem according to the ingress time to the event time for the data.
The data gets accumulated based on the configured time resolution.
"""
import sys
import logging
import logging.config
from configparser import ConfigParser

from osiris.core.enums import TimeResolution
from osiris.core.instrumentation import TracerConfig
from osiris.core.io import PrometheusClient

from .transform import TransformIngestTime2EventTime

logger = logging.getLogger(__file__)


# pylint: disable=too-many-locals
def __get_pipeline(config, credentials_config) -> TransformIngestTime2EventTime:
    account_url = config['Azure Storage']['account_url']
    filesystem_name = config['Azure Storage']['filesystem_name']

    tenant_id = credentials_config['Authorization']['tenant_id']
    client_id = credentials_config['Authorization']['client_id']
    client_secret = credentials_config['Authorization']['client_secret']

    source = config['Datasets']['source']
    destination = config['Datasets']['destination']
    date_key_name = config['Datasets']['date_key_name']
    date_format = config['Datasets']['date_format']
    time_resolution = TimeResolution[config['Datasets']['time_resolution']]
    max_files = int(config['Pipeline']['max_files'])

    tracer_config = TracerConfig(service_name=config['Jaeger Agent']['name'],
                                 reporting_host=config['Jaeger Agent']['reporting_host'],
                                 reporting_port=config['Jaeger Agent']['reporting_port'])

    prometheus_client = PrometheusClient(environment=config['Prometheus']['environment'],
                                         name=config['Prometheus']['name'],
                                         hostname=config['Prometheus']['hostname'])

    try:
        return TransformIngestTime2EventTime(storage_account_url=account_url,
                                             filesystem_name=filesystem_name,
                                             tenant_id=tenant_id,
                                             client_id=client_id,
                                             client_secret=client_secret,
                                             source_dataset_guid=source,
                                             destination_dataset_guid=destination,
                                             date_format=date_format,
                                             date_key_name=date_key_name,
                                             time_resolution=time_resolution,
                                             max_files=max_files,
                                             tracer_config=tracer_config,
                                             prometheus_client=prometheus_client)
    except Exception as error:  # noqa pylint: disable=broad-except
        logger.error('Error occurred while initializing pipeline: %s', error)
        sys.exit(-1)


def main():
    """
    The main function which runs the transformation.
    """
    config = ConfigParser()
    config.read(['conf.ini', '/etc/osiris/conf.ini'])
    credentials_config = ConfigParser()
    credentials_config.read(['credentials.ini', '/vault/secrets/credentials.ini'])

    logging.config.fileConfig(fname=config['Logging']['configuration_file'],  # type: ignore
                              disable_existing_loggers=False)

    pipeline = __get_pipeline(config=config, credentials_config=credentials_config)

    # To disable azure INFO logging from Azure
    logger_labels = ['apache_beam.runners.portability.fn_api_runner.fn_runner',
                     'azure.core.pipeline.policies.http_logging_policy',
                     'azure.identity._internal.get_token_mixin',
                     'apache_beam.runners.portability.fn_api_runner.translations',
                     'apache_beam.runners.worker.statecache',
                     'apache_beam.runners.portability.fn_api_runner.worker_handlers']
    for logger_label in logger_labels:
        logging.getLogger(logger_label).setLevel(logging.WARNING)

    logger.info('Running the ingress2event_time transformation.')
    try:
        pipeline.transform()
    except Exception as error:  # noqa pylint: disable=broad-except
        logger.error('Error occurred while running pipeline: %s', error)
        sys.exit(-1)

    logger.info('Finished running the ingress2event_time transformation.')


if __name__ == '__main__':
    main()
