"""
Module to handle pipeline for time series.
Transforming from ingest time (ingress) to event time (egress)
"""
import logging
from typing import Optional

import apache_beam as beam
import apache_beam.transforms.core as beam_core
from apache_beam.options.pipeline_options import PipelineOptions
from osiris.core.azure_client_authorization import ClientAuthorization

from osiris.core.enums import TimeResolution
from osiris.core.instrumentation import TracerClass, TracerConfig, TracerDoFn
from osiris.core.io import PrometheusClient
from osiris.pipelines.azure_data_storage import Dataset
from osiris.pipelines.file_io_connector import DatalakeFileSource, FileBatchController
from osiris.pipelines.transformations import ConvertEventToTuple, UploadEventsToDestination, ConvertToDict, \
    JoinUniqueEventData


logger = logging.getLogger(__file__)


class TransformIngestTime2EventTime:
    """
    Class to create pipelines for time series data
    """
    # pylint: disable=too-many-arguments, too-many-instance-attributes, too-few-public-methods
    def __init__(self, storage_account_url: str, filesystem_name: str, tenant_id: str, client_id: str,
                 client_secret: str, source_dataset_guid: str, destination_dataset_guid: str, date_format: str,
                 date_key_name: str, time_resolution: TimeResolution, max_files: int,
                 tracer_config: TracerConfig,
                 prometheus_client: Optional[PrometheusClient] = None):
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
        :param tracer_config: Configuration of Jaeger Tracer
        :param prometheus_client: An optional Prometheus Client to generate metrics
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
        self.tracer_config = tracer_config
        self.prometheus_client = prometheus_client

    def transform(self):
        """
        Creates a pipeline to transform from ingest time (ingress) to event on a daily time (egress).

        It runs in batches of max_files and iterates until no more files to process in ingress.
        """
        logger.info('Initializing TransformIngestTime2EventTime.transform')
        tracer = TracerClass(self.tracer_config)

        client_auth = ClientAuthorization(tenant_id=self.tenant_id,
                                          client_id=self.client_id,
                                          client_secret=self.client_secret)

        dataset_source = Dataset(client_auth=client_auth.get_local_copy(),
                                 account_url=self.storage_account_url,
                                 filesystem_name=self.filesystem_name,
                                 guid=self.source_dataset_guid,
                                 prometheus_client=self.prometheus_client)

        dataset_destination = Dataset(client_auth=client_auth.get_local_copy(),
                                      account_url=self.storage_account_url,
                                      filesystem_name=self.filesystem_name,
                                      guid=self.destination_dataset_guid,
                                      prometheus_client=self.prometheus_client)

        file_batch_controller = FileBatchController(dataset=dataset_source,
                                                    max_files=self.max_files)

        while file_batch_controller.more_files_to_process():
            logger.info('TransformIngestTime2EventTime.transform: start batch')

            with tracer.start_span('Batch') as span:
                carrier_ctx = tracer.get_carrier(span)

                paths = file_batch_controller.get_batch()
                for path in paths:
                    span.set_tag('path', path)

                datalake_connector = DatalakeFileSource(dataset=dataset_source,
                                                        file_paths=paths)

                with beam.Pipeline(options=PipelineOptions(['--runner=DirectRunner'])) as pipeline:
                    _ = (
                        pipeline  # noqa
                        | 'Read from filesystem' >> beam.io.Read(datalake_connector)  # noqa
                        | 'Convert to dict' >> beam_core.ParDo(TracerDoFn(ConvertToDict(),  # noqa
                                                                          carrier_ctx=carrier_ctx))  # noqa
                        | 'Tuple elements' >> beam_core.ParDo(TracerDoFn(ConvertEventToTuple(self.date_key_name,  # noqa
                                                                                             self.date_format,  # noqa
                                                                                             self.time_resolution),
                                                                         carrier_ctx=carrier_ctx))  # noqa
                        | 'Group by date' >> beam_core.GroupByKey()  # noqa
                        | 'Merge Storage' >> beam_core.ParDo(TracerDoFn(JoinUniqueEventData(dataset_destination,  # noqa
                                                                                            self.time_resolution),
                                                                        carrier_ctx=carrier_ctx,
                                                                        tag_index=0,
                                                                        tag_name='merge_time'))  # noqa
                        | 'Write Blob' >> beam_core.ParDo(TracerDoFn(UploadEventsToDestination(dataset_destination,  # noqa pylint: disable=line-too-long
                                                                                               self.time_resolution),
                                                                     carrier_ctx=carrier_ctx,
                                                                     tag_index=0,
                                                                     tag_name='upload_dest'))  # noqa
                    )

                logger.info('TransformIngestTime2EventTime.transform: batch processed and saving state')
                file_batch_controller.save_state()

        tracer.close()
        logger.info('TransformIngestTime2EventTime.transform: Finished')
