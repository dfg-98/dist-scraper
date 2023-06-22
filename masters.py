import re
from multiprocessing import Process, Queue

import zmq

import messages
from logger import get_logger
from service_discovery import discover_peer, get_masters
from sockets import CloudPickleContext, CloudPickleSocket, no_block_REQ

log = get_logger("Master")


def get_remote_tasks(master, tasks_queue, signkey=None):
    """
    Process that ask to other seed for his tasks.
    """
    context = CloudPickleContext()
    sock: CloudPickleSocket = no_block_REQ(context, timeout=1000)

    sock.connect(f"tcp://{master}")

    for _ in range(2):
        try:
            sock.send_data((messages.GET_TASKS,), signkey=signkey)

            response = sock.recv_data(signkey=signkey)
            log.info(f"Tasks received", "Get Remote Tasks")
            assert isinstance(
                response, dict
            ), f"Bad response, expected dict received {type(response)}"
            tasks_queue.put(response)
            break
        except zmq.error.Again as e:
            log.debug(e, "Get Remote Tasks")
        except AssertionError as e:
            log.debug(e, "Get Remote Tasks")
        except Exception as e:
            log.error(e, "Get Remote Tasks")
    sock.close()
    tasks_queue.put(None)


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
                    f"Parameter seed inserted is not a valid ip_address:port_number"
                )
                master = None

        if master is None:
            # //TODO: Change times param in production
            master, network = discover_peer(3, log)
            log.info(f"Master founded: {master}", "login")
            if master == "":
                self.tasks = {}
                log.info("Login finished", "login")
                return network

        masters_queue = Queue()
        process_get_masters = Process(
            target=get_masters,
            name="Get Masters",
            args=(
                master,
                discover_peer,
                (self.addr, self.port),
                True,
                masters_queue,
                log,
            ),
            kwargs={"signkey": self.signkey},
        )
        process_get_masters.start()
        tmp = masters_queue.get()
        # If Get Seeds fails to connect to a seed for some reason
        if tmp is not None:
            self.masters.extend([s for s in tmp if s not in self.masters])
        process_get_masters.terminate()

        tasks_queue = Queue()
        process_get_remote_tasks = Process(
            target=get_remote_tasks,
            name="Get Remote Tasks",
            args=(master, tasks_queue),
            kwargs={"signkey": self.signkey},
        )
        process_get_remote_tasks.start()
        tasks = tasks_queue.get()
        process_get_remote_tasks.terminate()
        self.tasks = {} if tasks is None else tasks
        log.info("Login finished", "login")
        return network