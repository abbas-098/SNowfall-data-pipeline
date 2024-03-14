import time
from snowfall_pipeline.common_utilities.snowfall_logger import SnowfallLogger

def transformation_timer(func):
    """Decorator that measures the execution time of a function and logs it.

    Args:
        func (callable): The function to be timed.

    Returns:
        callable: The decorated function.
    """
    def wrapper(*args, **kwargs):
        """Wrapper function to measure execution time and log it.

        Args:
            *args: Positional arguments passed to the decorated function.
            **kwargs: Keyword arguments passed to the decorated function.

        Returns:
            Any: The result of the decorated function.
        """
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger = SnowfallLogger.get_logger()
        logger.info(f"Function {func.__name__} took {end_time - start_time} seconds to complete.")
        return result
    return wrapper