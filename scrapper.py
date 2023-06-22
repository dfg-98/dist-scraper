import logging
import re
from multiprocessing import Process, Queue

from service_discovery import discover_peer, get_masters

log = logging.getLogger("Scraper")


class Scraper:
    """
    Represents a scraper, the worker node in the Scraper network.
    """

    def __init__(self, address, port):
        self.addr = address
        self.port = port
        self.curTask = []

        log.info(f"Scrapper created", "init")

    def login(self, master=None):
        """
        Login the node in the system.
        """
        network = True
        if master is not None:
            # ip_address:port_number
            regex = re.compile("\d{,3}\.\d{,3}\.\d{,3}\.\d{,3}:\d+")
            try:
                assert regex.match(master).end() == len(master)
            except (AssertionError, AttributeError):
                log.error(
                    f"Parameter seed inserted is not a valid ip_address:port_number"
                )
                master = None

        if master is None:
            # //TODO: Change times param in production
            log.debug("Discovering seed nodes", "login")
            master, network = discover_peer(3, log)
            if master == "":
                self.masters = list()
                log.info("Login finished", "login")
                return network

        masters_queue = Queue()
        p_get_masters = Process(
            target=get_masters,
            name="Get Masters",
            args=(
                master,
                discover_peer,
                (self.addr, self.port),
                False,
                masters_queue,
                log,
            ),
        )
        p_get_masters.start()
        self.masters = masters_queue.get()
        p_get_masters.terminate()

        log.info("Login finished", "login")
        return network