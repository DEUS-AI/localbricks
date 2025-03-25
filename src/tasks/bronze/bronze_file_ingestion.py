from constants import cluster_keys
from deus_lib.abstract.pipeline_factory import PipelineFactory
from deus_lib.utils.LoggerSingleton import LoggerSingleton

def main():

    LoggerSingleton.configure()

    layer = 'bronze_layer'
    pipeline = PipelineFactory().create_pipeline(layer=layer)
    pipeline.run_event_file_driven_pipeline(
        cleaning_args={'cluster_keys': cluster_keys},
        write_args={'cluster_keys': cluster_keys}
        )

if __name__ == '__main__':
    main()
