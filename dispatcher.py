import re
from multiprocessing import Process, Queue

from logger import get_logger
from service_discovery import discover_peer, get_masters

log = get_logger("Master")


class Dispatcher:
    """
    Represents a client to the services of the Scrapper.
    """

    def __init__(
        self, urls, uuid, address="127.0.0.1", port=4142, depth=1, signkey=None
    ):
        self.depth = depth
        self.originals = set(urls)
        self.urls = list(self.originals)
        self.old = {url for url in self.originals}
        self.uuid = uuid
        self.idToLog = str(uuid)[:10]
        self.address = address
        self.port = port
        self.signkey = signkey

        log.info(f"Dispatcher created with uuid {uuid}", "Init")

    def login(self, master):
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
                    f"Parameter master inserted is not a valid ip_address:port_number"
                )
                master = None

        if master is None:
            # //TODO: Change times param in production
            log.debug("Discovering seed nodes", "login")
            master, network = discover_peer(3, log)
            if master == "":
                log.error(
                    "Login failed, get the address of a active master node or connect to the same network that the service",
                    "login",
                )
                return False

        masters_queue = Queue()
        get_masters_process = Process(
            target=get_masters,
            name="Get Masters",
            args=(
                master,
                discover_peer,
                (self.address, self.port),
                False,
                masters_queue,
                log,
            ),
            kwargs={"signkey": self.signkey},
        )
        get_masters_process.start()
        self.masters = masters_queue.get()
        get_masters_process.terminate()

        if not len(self.masters):
            log.error(
                "Login failed, get the address of a active master node or connect to the same network that the service",
                "login",
            )
            return False

        log.info("Login finished", "login")
        return network