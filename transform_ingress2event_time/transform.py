"""
Transforms data structured in the filesystem according to the ingress time to the event time for the data.
The data gets accumulated daily.
"""
import argparse
import sys
from datetime import datetime

from osiris.core.configuration import ConfigurationWithCredentials
from osiris.core.enums import TimeResolution
from osiris.pipelines.pipeline_timeseries import PipelineTimeSeries


configuration = ConfigurationWithCredentials(__file__)
config = configuration.get_config()
credentials_config = configuration.get_credentials_config()
logger = configuration.get_logger()


def __init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Transform from ingress time to event time accumulated daily')

    parser.add_argument('--ingress_time', type=str, default=None, help='the ingress time to start the ingress from.')

    return parser


def __get_pipeline() -> PipelineTimeSeries:
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

    try:
        return PipelineTimeSeries(storage_account_url=account_url,
                                  filesystem_name=filesystem_name,
                                  tenant_id=tenant_id,
                                  client_id=client_id,
                                  client_secret=client_secret,
                                  source_dataset_guid=source,
                                  destination_dataset_guid=destination,
                                  date_format=date_format,
                                  date_key_name=date_key_name,
                                  time_resolution=time_resolution)
    except Exception as error:  # noqa pylint: disable=broad-except
        logger.error('Error occurred while initializing pipeline: %s', error)
        sys.exit(-1)


def main():
    """
    The main function which runs the transformation.
    """
    argparser = __init_argparse()
    args, _ = argparser.parse_known_args()
    pipeline = __get_pipeline()
    logger.info('Running the ingress2event_time transformation.')
    try:
        if args.ingress_time and args.ingress_time != '':
            ingress_time = datetime.strptime(args.ingress_time, '%Y-%m-%dT%H')
            pipeline.transform_ingest_time_to_event_time(ingress_time)
        else:
            pipeline.transform_ingest_time_to_event_time()
    except Exception as error:  # noqa pylint: disable=broad-except
        logger.error('Error occurred while running pipeline: %s', error)
        sys.exit(-1)

    logger.info('Finished running the ingress2event_time transformation.')


if __name__ == '__main__':
    main()
