from deus_lib.abstract.pipeline_factory import PipelineFactory
from deus_lib.utils.LoggerSingleton import LoggerSingleton

def main():

    LoggerSingleton.configure()

    layer = 'landing_layer'
    pipeline = PipelineFactory().create_pipeline(layer=layer)
    pipeline.main(
        )

if __name__ == '__main__':
    main()