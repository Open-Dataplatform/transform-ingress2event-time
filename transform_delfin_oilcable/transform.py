"""
Module to handle pipeline for timeseries
"""
from abc import ABC
from datetime import datetime
import logging
from typing import List, Tuple
import json

import pandas as pd
import apache_beam as beam
import apache_beam.transforms.core as beam_core
from apache_beam.options.pipeline_options import PipelineOptions
from azure.core.exceptions import ResourceNotFoundError

from osiris.core.azure_client_authorization import ClientAuthorization
from osiris.core.enums import TimeResolution
from osiris.pipelines.azure_data_storage import DataSets
from osiris.pipelines.file_io_connector import DatalakeFileSource
from osiris.pipelines.transformations import ConvertEventToTuple, UploadEventsToDestination

from osiris.core.configuration import ConfigurationWithCredentials


configuration = ConfigurationWithCredentials(__file__)
logger = configuration.get_logger()

tmp = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
tmp.setLevel(logging.WARNING)

class _PrintEventData(beam_core.DoFn, ABC):
    """"
    Takes a list of events and join it with processed events, if such exists, for the particular event time.
    It will only keep unique pairs.
    """
    def __init__(self):
        super().__init__()

    def process(self, element, *args, **kwargs) -> List[Tuple]:
        """
        Overwrites beam.DoFn process.
        """
        logger.info(len(element))
        
        df = pd.DataFrame.from_records(element)

        return ["nice"]


class TransformDelfinOilcable:
    """
    Class to create pipelines for time series data
    """
    # pylint: disable=too-many-arguments, too-many-instance-attributes, too-few-public-methods
    def __init__(self, storage_account_url: str, filesystem_name: str, tenant_id: str, client_id: str,
                 client_secret: str, source_dataset_guid: str, destination_dataset_guid: str, max_files: int):
        """
        :param storage_account_url: The URL to Azure storage account.
        :param filesystem_name: The name of the filesystem.
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param source_dataset_guid: The GUID for the source dataset.
        :param destination_dataset_guid: The GUID for the destination dataset.
        :param date_format: The date format used in the time series.
        :param date_key_name: The key in the record containing the date.
        :param time_resolution: The time resolution to store the data in the destination dataset with.
        :param max_files: Number of files to process in every pipeline run.

        """
        if None in [storage_account_url, filesystem_name, tenant_id, client_id, client_secret, source_dataset_guid,
                    destination_dataset_guid, max_files]:
            raise TypeError

        self.storage_account_url = storage_account_url
        self.filesystem_name = filesystem_name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.source_dataset_guid = source_dataset_guid
        self.destination_dataset_guid = destination_dataset_guid
        self.max_files = max_files

    def transform(self, ingest_time: datetime = None):
        """
        Creates a pipeline to transform from ingest time to event on a daily time.
        :param ingest_time: the ingest time to parse - default to current time
        """

        client_auth = ClientAuthorization(self.tenant_id, self.client_id, self.client_secret)

        datalake_connector = DatalakeFileSource(credential=client_auth.get_credential_sync(),
                                                account_url=self.storage_account_url,
                                                filesystem_name=self.filesystem_name,
                                                guid=self.source_dataset_guid,
                                                ingest_time=ingest_time,
                                                max_files=self.max_files)

        if datalake_connector.estimate_size() == 0:
            return

        spark = PipelineOptions([
            "--runner=PortableRunner",
            "--job_endpoint=localhost:8099",
            "--environment_type=LOOPBACK"
        ])

        flink = PipelineOptions([
            "--runner=PortableRunner",
            "--job_endpoint=localhost:7099",
            "--environment_type=LOOPBACK"
        ])

        direct = PipelineOptions(['--runner=DirectRunner'])

        with beam.Pipeline(options=spark) as pipeline:
            _ = (
                pipeline  # noqa
                | 'read from filesystem' >> beam.io.Read(datalake_connector)  # noqa
                | 'Parse JSON' >> beam_core.Map(lambda x: json.loads(x))  # noqa pylint: disable=unnecessary-lambda
                | 'convert to pandas' >> beam_core.ParDo(_PrintEventData()) # noqa
            )

        datalake_connector.close()
