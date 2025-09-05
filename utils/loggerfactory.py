import logging
from threading import Lock

try:
    from google.cloud import logging as gcp_logging
    GCP_LOGGING_AVAILABLE = True
except ImportError:
    GCP_LOGGING_AVAILABLE = False

class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[94m",    # Blue
        logging.INFO: "\033[92m",     # Green
        logging.WARNING: "\033[93m",  # Yellow
        logging.ERROR: "\033[91m",    # Red
        logging.CRITICAL: "\033[95m", # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"

class LoggerFactory:
    _instance = None
    _lock = Lock()
    _loggers = {}

    def __new__(cls):
        print(cls._lock)
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(LoggerFactory, cls).__new__(cls)
        return cls._instance
 
    def get_logger(self, name: str, use_gcp: bool = False) -> logging.Logger:
        key = f"{name}_gcp" if use_gcp else name
        if key not in self._loggers:
            logger = logging.getLogger(name)
            if use_gcp and GCP_LOGGING_AVAILABLE:
                client = gcp_logging.Client()
                handler = client.get_default_handler()
                logger.addHandler(handler)
            else:
                if not logger.hasHandlers():
                    handler = logging.StreamHandler()
                    formatter = ColorFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                    handler.setFormatter(formatter)
                    logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            self._loggers[key] = logger
        return self._loggers[key]
