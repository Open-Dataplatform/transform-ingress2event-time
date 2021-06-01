"""
Module to handle pipeline for timeseries
"""
from abc import ABC
from datetime import datetime
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


class _JoinUniqueEventData(beam_core.DoFn, ABC):
    """"
    Takes a list of events and join it with processed events, if such exists, for the particular event time.
    It will only keep unique pairs.
    """
    def __init__(self, datasets: DataSets):
        super().__init__()

        self.datasets = datasets

    def process(self, element, *args, **kwargs) -> List[Tuple]:
        """
        Overwrites beam.DoFn process.
        """
        date = pd.to_datetime(element[0])
        events = element[1]
        try:
            processed_events = self.datasets.read_events_from_destination(date)
            joined_events = events + processed_events
            # Only keep unique elements in the list
            joined_events = [i for n, i in enumerate(joined_events) if i not in joined_events[n + 1:]]

            return [(date, joined_events)]
        except ResourceNotFoundError:
            return [(date, events)]


class TransformIngestTime2EventTime:
    """
    Class to create pipelines for time series data
    """
    # pylint: disable=too-many-arguments, too-many-instance-attributes, too-few-public-methods
    def __init__(self, storage_account_url: str, filesystem_name: str, tenant_id: str, client_id: str,
                 client_secret: str, source_dataset_guid: str, destination_dataset_guid: str, date_format: str,
                 date_key_name: str, time_resolution: TimeResolution, max_files: int):
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
                    destination_dataset_guid, time_resolution, date_format, date_key_name, max_files]:
            raise TypeError

        self.storage_account_url = storage_account_url
        self.filesystem_name = filesystem_name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.source_dataset_guid = source_dataset_guid
        self.destination_dataset_guid = destination_dataset_guid
        self.time_resolution = time_resolution
        self.date_format = date_format
        self.date_key_name = date_key_name
        self.max_files = max_files

    def transform(self, ingest_time: datetime = datetime.utcnow()):
        """
        Creates a pipeline to transform from ingest time to event on a daily time.
        :param ingest_time: the ingest time to parse - default to current time
        """
        client_auth = ClientAuthorization(self.tenant_id, self.client_id, self.client_secret)

        datasets = DataSets(self.storage_account_url, self.filesystem_name,
                            self.source_dataset_guid, self.destination_dataset_guid,
                            client_auth.get_credential_sync(), self.time_resolution)

        while True:

            datalake_connector = DatalakeFileSource(credential=client_auth.get_credential_sync(),
                                                    account_url=self.storage_account_url,
                                                    filesystem_name=self.filesystem_name,
                                                    guid=self.source_dataset_guid,
                                                    ingest_time=ingest_time,
                                                    max_files=self.max_files)

            if datalake_connector.estimate_size() == 0:
                break

            with beam.Pipeline(options=PipelineOptions(['--runner=DirectRunner'])) as pipeline:
                _ = (
                    pipeline  # noqa
                    | 'read from filesystem' >> beam.io.Read(datalake_connector)  # noqa
                    | 'Convert from JSON' >> beam_core.Map(lambda x: json.loads(x))  # noqa pylint: disable=unnecessary-lambda
                    | 'Create tuple for elements' >> beam_core.ParDo(ConvertEventToTuple(self.date_key_name,  # noqa
                                                                                         self.date_format,  # noqa
                                                                                         self.time_resolution))  # noqa
                    | 'Group by date' >> beam_core.GroupByKey()  # noqa
                    | 'Merge from Storage' >> beam_core.ParDo(_JoinUniqueEventData(datasets))  # noqa
                    | 'Write to Storage' >> beam_core.ParDo(UploadEventsToDestination(datasets))  # noqa
                )

            datalake_connector.close()
