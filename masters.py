import re
import time
from multiprocessing import Process, Queue
from threading import Lock, Thread

import zmq

import messages
from logger import get_logger
from service_discovery import discover_peer, get_masters
from sockets import CloudPickleContext, CloudPickleSocket, no_block_REQ

log = get_logger("Master")

lock_subscribers = Lock()


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


def push_task(push_queue, addr, signkey=None):
    """
    Process that push tasks to workers and notify pulled tasks.
    """
    context = CloudPickleContext()
    sock: CloudPickleSocket = context.socket(zmq.PUSH)
    sock.bind(f"tcp://{addr}")

    for url in iter(push_queue.get, messages.STOP):
        log.debug(f"Pushing {url}", "Task Pusher")
        sock.send_data(url.encode(), signkey=signkey)


def task_publisher(addr, task_queue, signkey=None):
    """
    Process that publish tasks changes to others master nodes.
    """
    context = CloudPickleContext()
    sock: CloudPickleSocket = context.socket(zmq.PUB)
    sock.bind(f"tcp://{addr}")

    while True:
        try:
            # task: (flag, url, data)
            task = task_queue.get()
            if isinstance(task[0], bool):
                if isinstance(task[2][1], int):
                    log.debug(
                        f"Publish pulled task: ({task[0]}, {task[1]})", "Task Publisher"
                    )
                    sock.send_data([messages.PULLED_TASK, task], signkey=signkey)
                    continue

                header = task[2][0]
                log.debug(f"Publish {header} of {task[1]}", "Task Publisher")
                sock.send_data([header.encode(), (task[1], task[2][1])])

            elif task[0] == messages.PURGE:
                log.debug(f"Publish PURGE", "Task Publisher")
                sock.send_data([messages.PURGE, messages.JUNK], signkey=signkey)

            elif task[0] == messages.REMOVE:
                log.debug(f"Publish REMOVE", "Task Publisher")
                sock.send_data([messages.REMOVE, task[1]], signkey=signkey)

            else:
                log.debug(f"Publish master: ({task[0]}:{task[1]})", "Task Publisher")
                sock.send_data([messages.NEW_MASTER, task], signkey=signkey)

        except Exception as e:
            log.error(e, "Task Publisher")


def connect_to_publishers(sock, peer_queue):
    """
    Thread that connect subscriber socket to masters.
    """
    for addr, port in iter(peer_queue.get, messages.STOP):
        with lock_subscribers:
            log.debug(
                f"Connecting to master {addr}:{port + 3}", "Connect to Publishers"
            )
            sock.connect(f"tcp://{addr}:{port + 3}")


def disconnect_from_publishers(sock, peer_queue):
    """
    Thread that disconnect subscriber socket from masters.
    """
    for addr, port in iter(peer_queue.get, "STOP"):
        with lock_subscribers:
            log.debug(
                f"Disconnecting from master {addr}:{port + 3}",
                "Disconnect from Publishers",
            )
            sock.disconnect(f"tcp://{addr}:{port + 3}")


def task_subscriber(
    peer_queue,
    disconnect_queue,
    task_queue,
    master_queue,
    data_queue,
    purge_queue,
    signkey=None,
):
    """
    Process that subscribe to published tasks
    """
    context = CloudPickleContext()
    sock: CloudPickleSocket = context.socket(zmq.SUB)
    sock.setsockopt(zmq.SUBSCRIBE, messages.PULLED_TASK)
    sock.setsockopt(zmq.SUBSCRIBE, messages.NEW_MASTER)
    sock.setsockopt(zmq.SUBSCRIBE, messages.UPDATE)
    sock.setsockopt(zmq.SUBSCRIBE, messages.NEW_DATA)
    sock.setsockopt(zmq.SUBSCRIBE, messages.FORCED)
    sock.setsockopt(zmq.SUBSCRIBE, messages.REMOVE)
    sock.setsockopt(zmq.SUBSCRIBE, messages.REMOVE)

    connect_thread = Thread(
        target=connect_to_publishers,
        name="Connect to Publishers",
        args=(sock, peer_queue),
    )
    connect_thread.start()

    # disconnect_thread = Thread(target=disconnect_from_publishers, name="Disconnect from Publishers", args=(sock, disconnect_queue))
    # disconnect_thread.start()

    time.sleep(1)

    while True:
        try:
            with lock_subscribers:
                header, task = sock.recv_data(signkey=signkey)

                log.info(f"Received Subscribed message: {header}", "Task Subscriber")
                if header == messages.PULLED_TASK:
                    # task: (flag, url, data)
                    flag, url, data = task
                    task_queue.put((flag, url, data))
                elif header == messages.APPEND:
                    # task: (address, port)
                    addr, port = task
                    master_queue.put((messages.APPEND, (addr, port)))
                    peer_queue.put((addr, port))
                elif header == messages.REMOVE:
                    # task: (address, port)
                    addr, port = task
                    master_queue.put((messages.REMOVE, (addr, port)))
                    # disconnectQ.put((addr, port))
                elif header == "PURGE":
                    purge_queue.put(False)
                else:
                    # header: UPDATE, NEW_DATA, FORCED
                    # task: (url, data)
                    data_queue.put((header, task))
        except Exception as e:
            log.error(e, "Task Subscriber")


def worker_attender(pulled_queue, result_queue, failed_queue, addr, signkey=None):
    """
    Process that listen notifications from workers.
    """
    context = CloudPickleContext()
    sock: CloudPickleSocket = context.socket(zmq.REP)
    sock.bind(f"tcp://{addr}")

    while True:
        try:
            msg = sock.recv_data(signkey=signkey)
            if msg[0] == messages.PULLED:
                log.info(f"Message received: {msg}", "Worker Attender")
                # msg = PULLED, url, workerAddr
                pulled_queue.put((False, msg[1], msg[2]))
            elif msg[0] == messages.DONE:
                log.info(f"Message received: ({msg[0]}, {msg[1]})", "Worker Attender")
                # msg = DONE, url, html
                result_queue.put((True, msg[1], msg[2]))
            elif msg[0] == messages.FAILED:
                log.info(f"Message received: {msg}", "Worker Attender")
                # msg = FAILED, url, timesAttempted
                failed_queue.put((False, msg[1], msg[1]))

            # nothing important to send
            sock.send_data(messages.OK, signkey=signkey)
        except Exception as e:
            # Handle connection error
            log.error(e, "Worker Attender")
            continue


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