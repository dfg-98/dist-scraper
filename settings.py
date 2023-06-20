"""
Application Settings
~~~~~~~~~~~~~~~~
"""
import os

from utils import Configuration


class DistScrapperConfig(Configuration):
    defaults = {
        "magic": os.getenv("DIST_SCRAPPER_MAGIC", "nomagicondev"),
        "log_level": "INFO",
    }


def get_config(
    path=os.getenv("DIST_SCRAPER_CONF", "~/.dist_scrapper/configuration.json")
):
    rc = DistScrapperConfig(path)
    return rc