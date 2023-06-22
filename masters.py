from logger import get_logger

log = get_logger("Master")


class MasterNode:
    """
    Represents a master node, the node that receive and attend all client request.
    """

    def __init__(self, address, port, signkey=None):
        self.addr = address
        self.port = port
        self.masters = [(address, port)]
        self.package = dict()
        self.request = dict()
        self.signkey = signkey

        log.info(f"Master node created with address:{address}:{port}", "main")
