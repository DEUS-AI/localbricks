from constants import cluster_keys
from deus_lib.abstract.pipeline_factory import PipelineFactory
from deus_lib.utils.LoggerSingleton import LoggerSingleton

def main():

    LoggerSingleton.configure()

    layer = 'bronze_layer'
    merge_tasks = PipelineFactory().create_pipeline(layer=layer)
    merge_tasks.main()

if __name__ == '__main__':
    main()
