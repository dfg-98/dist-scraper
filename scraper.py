import logging
import os
import re
import time
from ctypes import c_int
from multiprocessing import Process, Queue, Value
from threading import Lock, Semaphore, Thread

import requests
import zmq

import messages
from service_discovery import discover_peer, find_masters, get_masters

from .sockets import CloudPickleContext, CloudPickleSocket, no_block_REQ

log = logging.getLogger("Scraper")

available_slaves = Value(c_int)

# Locks
lock_work = Lock()
lock_SocketPull = Lock()
lock_SocketNotifier = Lock()
counter_SocketPull = Semaphore(value=0)
counter_SocketNotifier = Semaphore(value=0)


def connect_to_masters(sock, inc, lock, counter, peer_queue, user):
    """
    Thread that connect <sock> socket to seeds.
    """
    for addr, port in iter(peer_queue.get, messages.STOP):
        with lock:
            log.debug(
                f"Connecting to master {addr}:{port + inc}",
                f"Connect to Masters -- {user} socket",
            )
            sock.connect(f"tcp://{addr}:{port + inc}")
            counter.release()
            log.info(
                f"Scrapper connected to master with address:{addr}:{port + inc})",
                f"Connect to Masters -- {user} socket",
            )


def disconnect_from_masters(sock, inc, lock, counter, peer_queue, user):
    """
    Thread that disconnect <sock> socket from masters.
    """
    for addr, port in iter(peer_queue.get, messages.STOP):
        with lock:
            log.debug(
                f"Disconnecting from master {addr}:{port + inc}",
                f"Disconnect from Masters -- {user} socket",
            )
            sock.disconnect(f"tcp://{addr}:{port + inc}")
            counter.acquire()
            log.info(
                f"Scrapper disconnected from master with address:{addr}:{port + inc})",
                f"Disconnect from Masters -- {user} socket",
            )


def notifier(notifications, peer_queue, dead_queue):
    """
    Process to send notifications of task's status to masters.
    """
    context = CloudPickleContext()
    socket: CloudPickleSocket = no_block_REQ(context)

    # Thread that connect REQ socket to masters
    connect_thread = Thread(
        target=connect_to_masters,
        name="Connect to Masters - Notifier",
        args=(
            socket,
            2,
            lock_SocketNotifier,
            counter_SocketNotifier,
            peer_queue,
            "Notifier",
        ),
    )
    connect_thread.start()

    # Thread that disconnect REQ socket from masters
    disconnect_thread = Thread(
        target=disconnect_from_masters,
        name="Disconnect from Masters - Notifier",
        args=(
            socket,
            2,
            lock_SocketNotifier,
            counter_SocketNotifier,
            dead_queue,
            "Notifier",
        ),
    )
    disconnect_thread.start()

    for msg in iter(notifications.get, messages.STOP):
        try:
            assert len(msg) == 3, "wrong notification"
        except AssertionError as e:
            log.error(e, "Worker Notifier")
            continue
        while True:
            try:
                with lock_SocketNotifier:
                    if counter_SocketNotifier.acquire(timeout=1):
                        log.debug(
                            f"Sending msg: ({msg[0]}, {msg[1]}, data) to a master",
                            "Worker Notifier",
                        )
                        # msg: (flag, url, data)
                        socket.send_data(msg)
                        # nothing important to receive
                        socket.recv_data()
                        counter_SocketNotifier.release()
                        break
            except zmq.error.Again as e:
                log.debug(e, "Worker Notifier")
                counter_SocketNotifier.release()
            except Exception as e:
                log.error(e, "Worker Notifier")
                counter_SocketNotifier.release()
            finally:
                time.sleep(0.5)


def listener(addr, port, queue, data):
    """
    Process to attend the verification messages sent by the master.
    """

    def puller():
        for flag, url in iter(queue.get, messages.STOP):
            with lock_work:
                try:
                    if flag:
                        data.append(url)
                    else:
                        data.remove(url)
                except Exception as e:
                    log.error(e, "puller")

    thread = Thread(target=puller)
    thread.start()
    socket = CloudPickleSocket().socket(zmq.REP)
    socket.bind(f"tcp://{addr}:{port}")

    while True:
        res = socket.recv_data()
        with lock_work:
            socket.send_data(res in data)


def slave(tasks, notifications, idx, verify_queue):
    """
    Child Process of Scrapper, responsable of downloading the urls.
    """
    while True:
        url = tasks.get()
        with available_slaves:
            available_slaves.value -= 1
        log.info(f"Child:{os.getpid()} of Scrapper downloading {url}", f"slave {idx}")
        for i in range(5):
            try:
                response = requests.get(url)
            except Exception as e:
                log.error(e, f"slave {idx}")
                if i == 4:
                    notifications.put((messages.FAILED, url, i))
                continue
            notifications.put((messages.DONE, url, response.content))
            verify_queue.put((False, url))
            break
        with available_slaves:
            available_slaves.value += 1


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

    def manage(self, slaves):
        """
        Start to manage childs-slaves.
        """
        context = CloudPickleContext()
        socket_pull = context.socket(zmq.PULL)

        masters_queue1 = Queue()
        masters_queue2 = Queue()
        for address in self.seeds:
            masters_queue1.put(address)
            masters_queue2.put(address)

        # Thread that connect pull socket to seeds
        connection_thread = Thread(
            target=connect_to_masters,
            name="Connect to Masters - Pull",
            args=(
                socket_pull,
                1,
                lock_SocketPull,
                counter_SocketPull,
                masters_queue1,
                "Pull",
            ),
        )
        connection_thread.start()

        pending_queue = Queue()
        to_disconnect_queue1 = Queue()
        to_disconnect_queue2 = Queue()

        # Thread that disconnect pull socket from seeds
        disconnect_thread = Thread(
            target=disconnect_from_masters,
            name="Disconnect from Seeds - Pull",
            args=(
                socket_pull,
                1,
                lock_SocketPull,
                counter_SocketPull,
                to_disconnect_queue1,
                "Notifier",
            ),
        )
        disconnect_thread.start()

        process_find_masters = Process(
            target=find_masters,
            name="Find Masters",
            args=(
                set(self.masters),
                [masters_queue1, masters_queue2],
                [to_disconnect_queue1, to_disconnect_queue2],
                log,
            ),
        )
        process_find_masters.start()

        notifications_queue = Queue()
        process_notifier = Process(
            target=notifier,
            name="Process Notifier",
            args=(notifications_queue, masters_queue2, to_disconnect_queue2),
        )
        process_notifier.start()

        process_listen = Process(
            target=listener,
            name="Process Listen",
            args=(self.addr, self.port, pending_queue, self.curTask),
        )
        process_listen.start()

        task_queue = Queue()
        log.info(f"Scrapper starting child processes", "manage")
        available_slaves.value = slaves
        for i in range(slaves):
            p = Process(
                target=slave, args=(task_queue, notifications_queue, i, pending_queue)
            )
            p.start()
            log.debug(
                f"Scrapper has started a child process with pid:{p.pid}", "manage"
            )

        addr = (self.addr, self.port)
        time.sleep(1)
        while True:
            # task: (client_addr, url)
            try:
                with available_slaves:
                    if available_slaves.value > 0:
                        log.debug(
                            f"Available Slaves: {available_slaves.value}", "manage"
                        )
                        with counter_SocketPull:
                            with lock_SocketPull:
                                url = socket_pull.recv_data()
                        with lock_work:
                            if url not in self.curTask:
                                task_queue.put(url)
                                notifications_queue.put((messages.PULLED, url, addr))
                                pending_queue.put((True, url))
                                log.debug(f"Pulled {url} in scrapper", "manage")
            except zmq.error.ZMQError as e:
                log.debug(f"No new messages to pull: {e}", "manage")
            time.sleep(1)

        process_notifier.terminate()
        process_find_masters.terminate()
        process_listen.terminate()
