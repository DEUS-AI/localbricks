import logging
import time
from deus_lib.utils.LoggerSingleton import LoggerSingleton
from typing import Any, Callable


# Ensure the logging is configured
LoggerSingleton.configure()

def log_method_data(func: Callable) -> Callable:
    """
    A decorator that logs method execution details, handling instance methods properly.

    Args:
        func (Callable): The method to be decorated.

    Returns:
        Callable: The wrapped method with logging.
    """
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        """
        Wrapper function that logs method execution details.

        Args:
            self (Any): The instance the method is bound to.
            *args (Any): Positional arguments for the method.
            **kwargs (Any): Keyword arguments for the method.

        Returns:
            Any: The result of the method execution.
        """
        logging.info(f"Started method {func.__name__} with args: {args}, kwargs: {kwargs}")
        start_time = time.time()

        # Execute the method
        result = func(self, *args, **kwargs)

        # Log after the method has finished
        elapsed_time = time.time() - start_time
        logging.info(f"Finished method {func.__name__}; execution time: {elapsed_time:.2f} seconds with result {result}")
        
        return result
    return wrapper