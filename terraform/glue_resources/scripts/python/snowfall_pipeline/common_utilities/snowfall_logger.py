import logging

class SnowfallLogger:
    """Base class for setting up a standardized logger."""
    
    _INSTANCE = None 

    def __new__(cls):
        """Create a new instance of SnowfallLogger."""
        if cls._INSTANCE is None: 
            cls._INSTANCE = super(SnowfallLogger, cls).__new__(cls)
            cls._INSTANCE.logger = cls._INSTANCE._setup_logging()
        return cls._INSTANCE

    def _setup_logging(self):
        """Set up logging configuration.

        Returns:
            logging.Logger: A configured logging.Logger object.
        """
        msg_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
        datetime_format = '%Y-%m-%d %H:%M:%S'
        logging.basicConfig(format=msg_format, datefmt=datetime_format, level=logging.INFO)
        logger = logging.getLogger(__name__)
        return logger

    @staticmethod
    def get_logger():
        """Static method to access the logger instance.

        Returns:
            logging.Logger: The configured logger instance.
        """
        return SnowfallLogger()._setup_logging()
