"""
Provides utility method to spawn a logger.
"""

import logging
import os

DEFAULT_LOGSIZE = int(os.getenv("LOGSIZE", "5242880"))
DEFAULT_LOGPATH = os.getenv("LOGPATH", "/tmp/log/tasq")
DEFAULT_FORMAT = os.getenv("LOGFMT", "%(asctime)s - %(name)s: %(message)s")
DEFAULT_LOGLEVEL = os.getenv("LOGLVL", "INFO")

datefmt = "%Y-%m-%d %H:%M:%S"

LOGLVLMAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}

BLACK = "\x1b[30m"
RED = "\x1b[31m"
GREEN = "\x1b[32m"
YELLOW = "\x1b[33m"
BLUE = "\x1b[34m"
MAGENTA = "\x1b[35m"
CYAN = "\x1b[36m"
RESET = "\x1b[0m"
BOLD = "\x1b[1m"
BLACKB = BLACK + BOLD
REDB = RED + BOLD
GREENB = GREEN + BOLD
YELLOWB = YELLOW + BOLD
BLUEB = BLUE + BOLD
MAGENTAB = MAGENTA + BOLD
CYANB = CYAN + BOLD

# Set default logging handler to avoid "No handler found" warnings.
try:
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


logging.getLogger(__name__).addHandler(NullHandler())


def LoggerFactory(name="root"):
    """
    Create a custom logger to use colors in the logs
    """
    logging.setLoggerClass(Logger)
    logging.basicConfig(format=DEFAULT_FORMAT, datefmt=datefmt)
    return logging.getLogger(name=name)


class Logger(logging.getLoggerClass()):
    def __init__(self, name="root", level=logging.NOTSET):
        self.debug_color = BLUEB
        self.info_color = YELLOWB
        self.error_color = REDB
        return super().__init__(name, level)

    def debug(self, msg, mth=""):
        super().debug(msg, extra={"color": self.debug_color, "method": mth})

    def info(self, msg, mth=""):
        super().info(msg, extra={"color": self.info_color, "method": mth})

    def error(self, msg, mth=""):
        super().error(msg, extra={"color": self.error_color, "method": mth})

    def change_color(self, method, color):
        setattr(self, f"{method}_color", color)


get_logger = LoggerFactory
