import logging

class LoggerSingleton:
    """
    A singleton class to handle logging throughout the application.
    
    This class ensures that the logging configuration is set up only once
    and can be used to configure logging across the application.
    """
    
    _is_configured = False

    @classmethod
    def configure(cls):
        """Configure logging if it has not been configured yet."""
        
        if not cls._is_configured:
            # Configure the root logger
            logging.basicConfig(level=logging.DEBUG,  # Set higher level to reduce verbosity globally
                                format='%(asctime)s - %(levelname)s - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')

            spark_logger = logging.getLogger('py4j')
            spark_logger.setLevel(logging.INFO)  # Restrict Spark's log output

            cls._is_configured = True