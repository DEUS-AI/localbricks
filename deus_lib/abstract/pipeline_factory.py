from deus_lib.bronze_factory.merge_dfs_task import MergeDataframes
#from deus_lib.silver_factory.silver_base_task import SilverBaseTask
from deus_lib.utils.common import initialize_job, parse_task_params
from deus_lib.bronze_factory.file_processor_base import IngestionFilesBase
from deus_lib.bronze_factory.csv_files_processor import IngestCsvFiles
from deus_lib.bronze_factory.xml_files_processor import IngestXmlFiles
from deus_lib.landing_factory.files_loader_task import FilesLoaderTask
from deus_lib.landing_factory.uncompress_and_load_zip import UncompressIngestZipTask
import logging
from typing import Union

class PipelineFactory():
    """Factory class for creating ingestion pipelines based on configuration."""

    def __init__(self):
        """Initializes the PipelineFactory."""

        logging.info("Initializing PipelineFactory")
        self.spark, self.dbutils = initialize_job()
        self.client_file_settings, self.task_params = parse_task_params()
        self.file_type = self.__retrieve_file_type()

    def __retrieve_file_type(self) -> str:

        file_type = self.client_file_settings['file_type']
        
        return file_type

    def create_pipeline(self, layer: str) -> Union[IngestionFilesBase, IngestXmlFiles, IngestCsvFiles]:
        """Creates an ingestion pipeline based on the provided configuration.

        Args:
            config(dict[str, Any]): A dictionary containing configuration parameters for the ingestion pipeline.

        Returns:
            IngestionFilesBase: An instance of the appropriate ingestion pipeline class.

        Raises:
            ValueError: If the specified layer is not implemented or if the configuration is invalid.
        """

        try:

            logging.debug(f"Creating pipeline for layer: {layer}, client_file_settings: {self.client_file_settings} and task_param: {self.task_params}")

            
            
            if layer == 'bronze_layer':

                process_config = {
                    'client_settings': self.client_file_settings,
                    'task_params': self.task_params,
                    'spark': self.spark,
                    'dbutils': self.dbutils,                    
                    'file_type': self.file_type, # format
                }

                
                if self.file_type == 'json':
                    
                    logging.info(f"Configuring IngestionFilesBase for file type: {self.file_type}")
                    return IngestionFilesBase(config=process_config)
            
                elif self.file_type == 'xml':
                    
                    logging.info(f"Configuring IngestXmlFiles")
                    return IngestXmlFiles(config=process_config)

                elif self.file_type == 'csv':

                    logging.info(f"Configuring IngestCsvFiles")
                    return IngestCsvFiles(config=process_config)
                
                elif self.file_type == 'merge':
                    
                    logging.info(f"Configuring merge task")
                    return MergeDataframes(config=process_config)

                else:

                    logging.error(f"Tasks with {self.file_type} not available")
                    raise ValueError(f"Tasks with {self.file_type} not available")

            elif layer == 'landing_layer':

                process_config = {
                    'client_settings': self.client_file_settings,
                    'task_params': self.task_params,
                    'spark': self.spark,
                    'dbutils': self.dbutils,
                    'file_type': self.file_type
                }

                if self.client_file_settings['compression'] == 'zip':

                    logging.info(f"Configuring IngestCsvFiles")
                    return UncompressIngestZipTask(config = process_config)
                
                elif self.client_file_settings['compression'] is False:


                    logging.info(f"Configuring ingestion of uncompressed files for landing")
                    return FilesLoaderTask(config = process_config)
                
                else:
                    raise ValueError(f"Compression format {self.client_file_settings['compression']} not implemented")
                
            elif layer == 'silver_layer':
                pass
                # logging.info(f"Configuring Silver layer")
                
                process_config = {
                    'client_settings': self.client_file_settings,
                    'task_params': self.task_params,
                    'spark': self.spark,
                    'dbutils': self.dbutils
                }
                                
                # return SilverBaseTask(config = process_config)
            
            else:

                logging.error(f'{layer} currently not implmented or does not exist')
                raise ValueError(f'{layer} currently not implmented or does not exist')
            
        except Exception as e:
            logging.error(f"Failed to create pipeline: {str(e)}")
            return None