"""
Module to handle pipeline for timeseries
"""
import json
from abc import ABC
from datetime import datetime
from io import BytesIO
from typing import List, Tuple

import pandas as pd
import apache_beam as beam
import apache_beam.transforms.core as beam_core
from apache_beam.options.pipeline_options import PipelineOptions
from azure.core.exceptions import ResourceNotFoundError
from osiris.core.azure_client_authorization import ClientAuthorization
from osiris.core.configuration import Configuration

from osiris.core.enums import TimeResolution
from osiris.core.io import get_file_path_with_respect_to_time_resolution
from osiris.pipelines.azure_data_storage import Dataset
from osiris.pipelines.file_io_connector import DatalakeFileSource, FileBatchController
from osiris.pipelines.transformations import ConvertEventToTuple, UploadEventsToDestination, ConvertToDict


configuration = Configuration(__file__)
logger = configuration.get_logger()


class _JoinUniqueEventData(beam_core.DoFn, ABC):
    """"
    Takes a list of events and join it with processed events, if such exists, for the particular event time.
    It will only keep unique pairs.
    """
    def __init__(self, datasets: Dataset, time_resolution: TimeResolution):
        super().__init__()

        self.datasets = datasets
        self.time_resolution = time_resolution

    def process(self, element, *args, **kwargs) -> List[Tuple]:
        """
        Overwrites beam.DoFn process.
        """
        date = pd.to_datetime(element[0])
        # we make sure there is no duplicates.
        events = []
        for event in element[1]:
            if event not in events:
                events.append(event)

        try:
            file_path = get_file_path_with_respect_to_time_resolution(date, self.time_resolution, 'data.parquet')
            file_content = self.datasets.read_file(file_path)
            processed_events_df = pd.read_parquet(BytesIO(file_content), engine='pyarrow')
            processed_events = json.loads(processed_events_df.to_json(orient='records'))

            for event in events:
                if event not in processed_events:
                    processed_events.append(event)

            return [(date, processed_events)]
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

    def transform(self, ingest_time: datetime = None):
        """
        Creates a pipeline to transform from ingest time to event on a daily time.
        :param ingest_time: the ingest time to parse - default to current time.
        """
        logger.info('Initializing TransformIngestTime2EventTime.transform')
        client_auth = ClientAuthorization(tenant_id=self.tenant_id,
                                          client_id=self.client_id,
                                          client_secret=self.client_secret)

        dataset = Dataset(client_auth=client_auth,
                          account_url=self.storage_account_url,
                          filesystem_name=self.filesystem_name,
                          guid=self.destination_dataset_guid)

        while True:
            logger.info('TransformIngestTime2EventTime.transform: while - init datalake_connector')

            file_batch_controller = FileBatchController(client_auth.get_local_copy(),
                                                        account_url=self.storage_account_url,
                                                        filesystem_name=self.filesystem_name,
                                                        guid=self.source_dataset_guid,
                                                        ingest_time=ingest_time,
                                                        max_files=self.max_files)

            datalake_connector = DatalakeFileSource(client_auth.get_local_copy(),
                                                    account_url=self.storage_account_url,
                                                    filesystem_name=self.filesystem_name,
                                                    file_paths=file_batch_controller.get_batch())

            if datalake_connector.estimate_size() == 0:
                logger.info('TransformIngestTime2EventTime.transform: break while-loop')
                break

            with beam.Pipeline(options=PipelineOptions(['--runner=DirectRunner'])) as pipeline:
                _ = (
                    pipeline  # noqa
                    | 'Read from filesystem' >> beam.io.Read(datalake_connector)  # noqa
                    | 'Convert to dict' >> beam_core.ParDo(ConvertToDict())  # noqa
                    | 'Create tuple for elements' >> beam_core.ParDo(ConvertEventToTuple(self.date_key_name,  # noqa
                                                                                         self.date_format,  # noqa
                                                                                         self.time_resolution))  # noqa
                    | 'Group by date' >> beam_core.GroupByKey()  # noqa
                    | 'Merge from Storage' >> beam_core.ParDo(_JoinUniqueEventData(dataset,  # noqa
                                                                                   self.time_resolution))  # noqa
                    | 'Write to Storage' >> beam_core.ParDo(UploadEventsToDestination(dataset, # noqa
                                                                                      self.time_resolution))  # noqa
                )

            logger.info('TransformIngestTime2EventTime.transform: beam-pipeline finished')
            file_batch_controller.save_state()

            if ingest_time:
                break
