"""
Customs errors and exceptions in this project
"""


class SignatureNotValidException(Exception):
    """Ad-hoc exception for invalid digest signature which doesn't pass the
    verification
    """

    pass


class CrawlException(Exception):
    """Ad-hoc exception for crawl errors"""

    def __init__(self, url, *args, **kwargs):
        self.url = url
        message = f"Error while crawling {url}"
        super().__init__(message, *args, **kwargs)
