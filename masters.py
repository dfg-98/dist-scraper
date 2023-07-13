import random
import re
import time
from multiprocessing import Process, Queue
from threading import Lock, Thread

import zmq

import messages
from logger import LOGLVLMAP, get_logger
from service_discovery import (
    broadcast_listener,
    discover_peer,
    get_masters,
    ping,
)
from sockets import CloudPickleContext, CloudPickleSocket, no_block_REQ
from utils import ConsistencyUnit, clock

log = get_logger("Master")

lock_subscribers = Lock()
lock_tasks = Lock()
lock_masters = Lock()
lock_clients = Lock()


def get_remote_tasks(master, tasks_queue, signkey=None):
    """
    Process that ask to other master for his tasks.
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
        sock.send_data(url, signkey=signkey)


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
    sock.setsockopt(zmq.SUBSCRIBE, messages.PULLED_TASK.encode())
    sock.setsockopt(zmq.SUBSCRIBE, messages.NEW_MASTER.encode())
    sock.setsockopt(zmq.SUBSCRIBE, messages.UPDATE.encode())
    sock.setsockopt(zmq.SUBSCRIBE, messages.NEW_DATA.encode())
    sock.setsockopt(zmq.SUBSCRIBE, messages.FORCED.encode())
    sock.setsockopt(zmq.SUBSCRIBE, messages.REMOVE.encode())
    sock.setsockopt(zmq.SUBSCRIBE, messages.REMOVE.encode())

    connect_thread = Thread(
        target=connect_to_publishers,
        name="Connect to Publishers",
        args=(sock, peer_queue),
    )
    connect_thread.start()

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
                    disconnect_queue.put((addr, port))
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


def quick_verification(address, url, t, queue, signkey=None):
    """
    Process that send a verification message to a worker
    to find out if he is working and report it.
    """
    context = CloudPickleContext()
    sock: CloudPickleSocket = no_block_REQ(context, timeout=t)
    ans = False
    try:
        addr, port = address
        sock.connect(f"tcp://{addr}:{port}")
        log.debug(f"Sending quick verification to {addr}:{port}", "Quick Verification")

        sock.send_data(url, signkey=signkey)

        ans = sock.recv_data(signkey=signkey)
        log.debug(
            f"Worker at {address} is alive. Is working on {url}: {ans}",
            "Quick Verification",
        )
    except zmq.error.Again:
        log.debug(f"Worker at {address} unavailable", "Quick Verification")
    except Exception as e:
        log.error(e, "Quick Verification")
    finally:
        queue.put(ans)


def verificator(queue, t, push_queue, signkey=None):
    """
    Process that manage the list of workers who should be verified.
    """

    for address, url in iter(queue.get, messages.STOP):
        ans_queue = Queue()
        quick_process = Process(
            target=quick_verification,
            args=(address, url, t, ans_queue),
            kwargs={"signkey": signkey},
        )
        quick_process.start()
        ans = ans_queue.get()
        quick_process.terminate()
        if not ans:
            push_queue.put(url)


def masters_verificator(masters, masters_queue, sleep_time, timeout=100, signkey=None):
    """
    Process that pings masters to see if they are alive
    """

    while True:
        data = list(masters)
        log.debug(f"Masters to ping: {masters=}")
        for m in data:
            # This process is useful to know if a master is dead too
            ping_queue = Queue()
            process_ping = Process(
                target=ping,
                name="Ping",
                args=(m, ping_queue, timeout, log),
                kwargs={
                    "signkey": signkey,
                },
            )
            process_ping.start()
            status = ping_queue.get()
            process_ping.terminate()
            if not status:
                log.debug(f"Put REMOVE master {m}")
                masters_queue.put((messages.REMOVE, m))
            else:
                log.debug(f"Put APPEND master {m}")
                masters_queue.put((messages.APPEND, m))

        time.sleep(sleep_time)


def task_manager(tasks, q, to_pub_queue, pub):
    """
    Thread that helps the master main process to update the tasks map.
    """
    while True:
        flag, url, data = q.get()
        with lock_tasks:
            if url in tasks and tasks[url][0]:
                # task already exists and is not finished
                # we can't update it
                log.debug(
                    f"Task {url} already exists and is not finished", "Task Manager"
                )
                continue
            tasks[url] = (flag, data)
            log.debug(f"Task {url} added", "Task Manager")
            # publish to other master
            if pub:
                log.debug(f"Publishing task {url}...", "Task Manager")
                to_pub_queue.put((flag, url, data))


def masters_manager(masters, q, self_address):
    """
    Thread that helps the master main process to update the masters list.
    """
    while True:
        cmd, address = q.get()
        with lock_masters:
            if cmd == messages.APPEND and address not in masters:
                masters.append(address)
                log.info(f"Appended master", "Masters Manager")
            elif (
                cmd == messages.REMOVE
                and address in masters
                and address != self_address
            ):
                masters.remove(address)
                log.info(f"Removed master", "Masters Manager")


def resource_manager(tasks, data_queue):
    """
    Thread that manage publications of masters nodes
    related to downloaded data.
    """
    while True:
        header, task = data_queue.get()
        url, data = task
        with lock_tasks:
            try:
                if not tasks[url][0]:
                    raise KeyError
                if header == messages.FORCED:
                    if tasks[url][1].data is not None:
                        tasks[url][1].update_data(data[0])
                    tasks[url][1].add_owner(data[1])
                elif header == messages.UPDATE:
                    log.debug(f"Updating data of {url}...", "Resource Manager")
                    tasks[url][1].update_data(data)
                elif header == messages.NEW_DATA:
                    tasks[url][1].add_owner(data)
            except KeyError:
                if header == messages.FORCED:
                    tasks[url] = (True, ConsistencyUnit(None, [data[1]]))
                elif header == messages.NEW_DATA:
                    tasks[url] = (True, ConsistencyUnit(None, [data]))


def consistency_unit_creator(
    tasks, address, result_queue, to_pub_queue, request, package, del_threshold
):
    """
    Thread that manage ConsistencyUnit creation.
    """
    while True:
        flag, url, data = result_queue.get()
        with lock_tasks:
            if flag:
                # it comes from Worker Attender
                if url in tasks and tasks[url][0]:
                    if tasks[url][1].data is not None:
                        # I have an old copy, update data
                        log.debug(
                            f"Updating data with url: {url}", "Consistency Unit Creator"
                        )
                        conit = tasks[url][1]
                        conit.update_data(data)
                        # UPDATE: call your conit's update_data with data
                        to_pub_queue.put((flag, url, (messages.UPDATE, data)))
                    else:
                        # I have the list of owners, but force replication of data, this is a rare case
                        log.debug(
                            f"Forcing to save data with url: {url}",
                            "Consistency Unit Creator",
                        )
                        conit = tasks[url][1]
                        conit.update_data(data)
                        conit.add_owner(address)
                        # FORCED: call your conit's update_data with data (in case of having a replica)
                        # and update owners
                        to_pub_queue.put(
                            (flag, url, (messages.FORCED, (data, address)))
                        )
                else:
                    # it seems that nobody have it. Save data
                    log.debug(
                        f"Saving data with url: {url}", "Consistency Unit Creator"
                    )
                    conit = ConsistencyUnit(data, owners=[address], limit=del_threshold)
                    tasks[url] = (True, conit)
                    # NEW_DATA: update owners of url's data with address
                    to_pub_queue.put((flag, url, (messages.NEW_DATA, address)))
            else:
                # It comes from dispatch, Replicate data
                log.debug(f"Replicating data of {url}...", "Consistency Unit Creator")
                tasks[url][1].update_data(data[0], data[1])
                data = data[0]
                tasks[url][1].add_owner(address)
                to_pub_queue.put((flag, url, (messages.NEW_DATA, address)))
            with lock_clients:
                if url in request:
                    for id in request[url]:
                        log.debug(f"Adding {url} to {id}", "Consistency Unit Creator")
                        package[id][url] = data


def remove_owner(tasks, remove_queue, to_pub_queue):
    """
    Thread that remove owner from conits that have it.
    """
    while True:
        o, url = remove_queue.get()
        with lock_tasks:
            if url in tasks and tasks[url][0]:
                tasks[url][1].remove_owner(o)
                log.debug(f"Owner {o} removed from conits", "Remove Owner")
            to_pub_queue.put((messages.REMOVE, o))


def purger(tasks, address, cycle, to_pub_queue, purge_queue, old_requests):
    """
    Thread that purge the downloaded data from tasks map when a time cycle occurs.
    """

    while True:
        clock_process = Process(target=clock, args=(cycle, purge_queue))
        clock_process.start()
        pub = purge_queue.get()
        clock_process.terminate()
        with lock_tasks:
            log.info("Starting purge", "Purger")
            with lock_clients:
                old_requests.clear()
            for url, value in tasks.items():
                if value[0]:
                    if value[1].data is not None and value[1].is_removable():
                        value[1].data = None
                        value[1].remove_owner(address)
                    value[1].add_live()
        log.debug(f"Tasks after purge: {tasks}", "Purger")
        log.info("Purge finished", "Purger")
        if pub:
            to_pub_queue.put((messages.PURGE,))


def get_data(
    url,
    address,
    owners,
    result_queue,
    remove_queue,
    masters_manager_queue,
    signkey=None,
):
    """
    Process that make a NOBLOCK request to know owners
    of url's data.
    """
    context = CloudPickleContext()
    sock: CloudPickleSocket = no_block_REQ(context, timeout=1000)

    random.shuffle(owners)
    for o in owners:
        sock.connect(f"tcp://{o[0]}:{o[1]}")
        try:
            log.debug(f"Requesting data to master: {o}", "Get Data")
            sock.send_data((messages.GET_DATA, url), signkey=signkey)

            ans = sock.recv_data(signkey=signkey)
            if ans == False:
                # rare case that 'o' don't have the data
                remove_queue.put((o, url))
                masters_manager_queue.put(("REMOVE", o))
                continue

            # ans: (data, lives)
            result_queue.put(ans)
            break

        except zmq.error.Again as e:
            log.debug(e, "Get Data")
            remove_queue.put((o, url))
            masters_manager_queue.put((messages.REMOVE, o))
        except Exception as e:
            log.error(e, "Get Data")
        finally:
            sock.disconnect(f"tcp://{o[0]}:{o[1]}")
    result_queue.put(False)


def clone_tasks(tasks: dict):
    """
    Copy of tasks, without heavy Consistency Units.
    """
    tasks_copy = dict()
    for key, value in tasks.items():
        if value[0]:
            tasks_copy[key] = (value[0], value[1].copy())
        else:
            tasks_copy[key] = value
    log.debug("Lite copy of tasks created", "clone_tasks")
    return tasks_copy


def replication_manager(
    tasks,
    address,
    remove_queue,
    new_masters_queue,
    result_queue,
    replication_limit,
    signkey=None,
):
    """Thread for replicate data"""
    while True:
        log.debug("Replication Cycle Start", "Replication Manager")
        for url in tasks:
            task = tasks[url]
            if task[0] and task[1].data == None:
                log.debug(f"Don't have local replica of {url}", "Replication Manager")
                # i don't have a local replica, ask owners
                get_data_queue = Queue()
                get_data_process = Process(
                    target=get_data,
                    name="Get Data",
                    args=(
                        url,
                        address,
                        task[1].owners,
                        get_data_queue,
                        remove_queue,
                        new_masters_queue,
                    ),
                    kwargs={"signkey": signkey},
                )
                get_data_process.start()
                data = get_data_queue.get()
                get_data_process.terminate()
                if data:
                    # data: (data, lives)
                    log.debug(
                        f"Hit on {url}. Total hits: {task[1].hits + 1}",
                        "serve",
                    )

                    if task[1].try_own(replication_limit):
                        # replicate
                        log.debug(f"Replicating data of {url}", "Replication Manager")
                        result_queue.put((False, url, data))

        time.sleep(conf["REPLICATION_DELAY"])


class MasterNode:
    """
    Represents a master node, the node that receive and attend all client request.
    """

    def __init__(
        self, address, port, deletion_threshold=5, replication_limit=2, signkey=None
    ):
        self.addr = address
        self.port = port
        self.masters = [(address, port)]
        self.package = dict()
        self.request = dict()
        self.signkey = signkey
        self.deletion_threshold = deletion_threshold
        self.replication_limit = replication_limit

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
                    f"Parameter master inserted is not a valid ip_address:port_number"
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
        # If Get Master fails to connect to a master for some reason
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
        log.debug(f"Tasks fetched from remotes: {self.tasks}", "login")
        return network

    def serve(self, broadcastPort):
        """
        Start to attend clients.
        """

        context = CloudPickleContext()
        sock: CloudPickleSocket = context.socket(zmq.REP)
        try:
            sock.bind(f"tcp://{self.addr}:{self.port}")
        except Exception as e:
            log.error(f"{e}, please check your connection, or the address", "serve")
            return

        push_queue = Queue()
        pulled_queue = Queue()
        result_queue = Queue()
        task_to_pub_queue = Queue()
        masters_queue = Queue()
        disconnect_queue = Queue()
        verification_queue = Queue()
        failed_queue = Queue()
        new_masters_queue = Queue()
        remove_queue = Queue()
        data_queue = Queue()
        purge_queue = Queue()

        tmp = self.masters.copy()
        tmp.remove((self.addr, self.port))
        for s in tmp:
            masters_queue.put(s)

        push_process = Process(
            target=push_task,
            name="Task Pusher",
            args=(push_queue, f"{self.addr}:{self.port + 1}"),
            kwargs={"signkey": self.signkey},
        )
        worker_attender_process = Process(
            target=worker_attender,
            name="Worker Attender",
            args=(
                pulled_queue,
                result_queue,
                failed_queue,
                f"{self.addr}:{self.port + 2}",
            ),
            kwargs={"signkey": self.signkey},
        )
        task_publisher_process = Process(
            target=task_publisher,
            name="Task Publisher",
            args=(f"{self.addr}:{self.port + 3}", task_to_pub_queue),
            kwargs={"signkey": self.signkey},
        )
        task_subscriber_process = Process(
            target=task_subscriber,
            name="Task Subscriber",
            args=(
                masters_queue,
                disconnect_queue,
                failed_queue,
                new_masters_queue,
                data_queue,
                purge_queue,
            ),
            kwargs={"signkey": self.signkey},
        )
        verifier_process = Process(
            target=verificator,
            name="Verificator",
            args=(verification_queue, 500, push_queue),
            kwargs={"signkey": self.signkey},
        )
        listener_process = Process(
            target=broadcast_listener,
            name="Broadcast Listener",
            args=((self.addr, self.port), broadcastPort, log),
        )

        task_manager1_thread = Thread(
            target=task_manager,
            name="Task Manager - PULLED",
            args=(self.tasks, pulled_queue, task_to_pub_queue, True),
        )
        task_manager2_thread = Thread(
            target=task_manager,
            name="Task Manager - FAILED",
            args=(self.tasks, failed_queue, task_to_pub_queue, False),
        )

        master_manager_thread = Thread(
            target=masters_manager,
            name="Masters Manager",
            args=(self.masters, new_masters_queue, (self.addr, self.port)),
        )
        resource_manager_thread = Thread(
            target=resource_manager,
            name="Resource Manager",
            args=(self.tasks, data_queue),
        )

        consistency_unit_creator_thread = Thread(
            target=consistency_unit_creator,
            name="Consistency Unit Creator",
            args=(
                self.tasks,
                (self.addr, self.port),
                result_queue,
                task_to_pub_queue,
                self.request,
                self.package,
                self.deletion_threshold,
            ),
        )
        remove_owner_thread = Thread(
            target=remove_owner,
            name="Remove Owner",
            args=(self.tasks, remove_queue, task_to_pub_queue),
        )
        purger_thread = Thread(
            target=purger,
            name="Purger",
            args=(
                self.tasks,
                (self.addr, self.port),
                600,  # 20s
                task_to_pub_queue,
                purge_queue,
                self.request,
            ),
        )

        replication_thread = Thread(
            target=replication_manager,
            name="Replication Manager",
            args=(
                self.tasks,
                (self.addr, self.port),
                remove_queue,
                new_masters_queue,
                result_queue,
                self.replication_limit,
            ),
            kwargs={"signkey": self.signkey},
        )
        process_masters_verification = Process(
            target=masters_verificator,
            name="Find Masters",
            args=(
                set(self.masters) - set({(self.addr, self.port)}),
                new_masters_queue,
            ),
            kwargs={"signkey": self.signkey, "sleep_time": 30},
        )

        push_process.start()
        worker_attender_process.start()
        task_publisher_process.start()
        task_subscriber_process.start()
        verifier_process.start()
        listener_process.start()
        process_masters_verification.start()

        task_manager1_thread.start()
        task_manager2_thread.start()
        master_manager_thread.start()
        resource_manager_thread.start()
        consistency_unit_creator_thread.start()
        remove_owner_thread.start()
        purger_thread.start()
        replication_thread.start()

        time.sleep(0.5)

        log.info(f"Starting server on {self.addr}:{self.port}", "serve")
        while True:
            try:
                msg = sock.recv_data(signkey=self.signkey)
                # log.debug(f"Received {msg}", "serve")
                if msg[0] == messages.URL:
                    _, id, url = msg
                    with lock_tasks:
                        with lock_clients:
                            if url not in self.request:
                                self.request[url] = set()
                            self.request[url].add(id)
                            if id not in self.package:
                                self.package[id] = dict()

                        try:
                            res = self.tasks[url]
                            log.debug(f"Res in self.tasks: {res}", "serve")

                            if not res[0]:
                                if isinstance(res[1], tuple):
                                    verification_queue.put((res[1], url))
                                elif url == res[1]:
                                    raise KeyError
                                else:
                                    self.tasks[url][1] += 1
                                    if self.tasks[url][1] == 10:
                                        raise KeyError

                            else:
                                if res[1].data == None:
                                    # i don't have a local replica, ask owners
                                    get_data_queue = Queue()
                                    get_data_process = Process(
                                        target=get_data,
                                        name="Get Data",
                                        args=(
                                            url,
                                            (self.addr, self.port),
                                            res[1].owners,
                                            get_data_queue,
                                            remove_queue,
                                            new_masters_queue,
                                        ),
                                        kwargs={"signkey": self.signkey},
                                    )
                                    get_data_process.start()
                                    data = get_data_queue.get()
                                    get_data_process.terminate()
                                    if data:
                                        # data: (data, lives)
                                        log.debug(
                                            f"Hit on {url}. Total hits: {res[1].hits + 1}",
                                            "serve",
                                        )
                                        with lock_clients:
                                            self.package[id][url] = data[0]
                                        if res[1].hit() and res[1].try_own(
                                            self.replication_limit
                                        ):
                                            # replicate
                                            log.debug(
                                                f"Replicating data of {url}", "serve"
                                            )
                                            result_queue.put((False, url, data))
                                    else:
                                        # nobody seems to have the data
                                        raise KeyError
                                else:
                                    # I have a local replica
                                    with lock_clients:
                                        self.package[id][url] = res[1].data
                                    res = (True, res[1].data)
                        except KeyError:
                            res = self.tasks[url] = [False, 0]
                            push_queue.put(url)
                    with lock_clients:
                        res = (messages.RESPONSE, self.package[id])
                        log.debug(f"Sending package of size {len(res[1])}", "serve")
                        sock.send_data(res, signkey=self.signkey)
                        self.package[id].clear()

                elif msg[0] == messages.GET_TASKS:
                    with lock_tasks:
                        log.info(
                            f"{messages.GET_TASKS} received, sending tasks", "serve"
                        )
                        sock.send_data(clone_tasks(self.tasks), signkey=self.signkey)

                elif msg[0] == messages.NEW_MASTER:
                    log.info(f"{messages.NEW_MASTER} received, saving new master...")
                    # addr = (address, port)
                    addr = tuple(msg[1])
                    with lock_masters:
                        if addr not in self.masters:
                            self.masters.append(addr)
                            masters_queue.put(addr)
                            task_to_pub_queue.put(addr)
                    sock.send_data(messages.OK, signkey=self.signkey)

                elif msg[0] == messages.GET_MASTERS:
                    log.info(
                        f"{messages.GET_MASTERS} received, sending masters", "serve"
                    )
                    with lock_masters:
                        log.debug(f"Sending Masters: {self.masters}", "serve")
                        masters_to_send = list(self.masters)
                        random.shuffle(masters_to_send)
                        sock.send_data(masters_to_send, signkey=self.signkey)

                elif msg[0] == messages.PING:
                    log.info(f"{messages.PING} received", "serve")
                    sock.send_data(messages.OK, signkey=self.signkey)

                elif msg[0] == messages.GET_DATA:
                    log.info(f"{messages.GET_DATA} received", "serve")
                    try:
                        rep = False
                        if (
                            self.tasks[msg[1]][0]
                            and self.tasks[msg[1]][1].data is not None
                        ):
                            rep = (
                                self.tasks[msg[1]][1].data,
                                self.tasks[msg[1]][1].lives,
                            )
                    except KeyError:
                        pass
                    log.debug(f"Sending response to {messages.GET_DATA}", "serve")
                    sock.send_data(rep, signkey=self.signkey)

                else:
                    sock.send_data(messages.UNKNOW, signkey=self.signkey)

            except KeyboardInterrupt:
                log.info("Stopping server", "serve")
                break
            except Exception as e:
                # Handle connection error
                log.error(e, "serve")
                time.sleep(5)

        push_process.terminate()
        worker_attender_process.terminate()
        task_publisher_process.terminate()
        task_subscriber_process.terminate()
        verifier_process.terminate()
        listener_process.terminate()
        process_masters_verification.terminate()


def main(args):
    log.setLevel(LOGLVLMAP[args.level])
    m = MasterNode(
        args.address,
        args.port,
        args.replication_limit,
        args.deletion_threshold,
        args.signkey,
    )
    if not m.login(args.master):
        log.info("You are not connected to a network", "main")
    m.serve(args.broadcast_port)


if __name__ == "__main__":
    import argparse

    from settings import get_config

    conf = get_config()

    parser = argparse.ArgumentParser(description="Master of a distibuted scraper")
    parser.add_argument(
        "-a", "--address", type=str, default="127.0.0.1", help="node address"
    )
    parser.add_argument("-p", "--port", type=int, default=8101, help="connection port")
    parser.add_argument(
        "-b",
        "--broadcast_port",
        type=int,
        default=conf["BROADCAST_PORT"],
        help="broadcast listener port (Default: 4142)",
    )
    parser.add_argument("-l", "--level", type=str, default="INFO", help="log level")
    parser.add_argument(
        "-m",
        "--master",
        type=str,
        default=None,
        help="address of a existing master node. Insert as ip_address:port_number",
    )
    parser.add_argument(
        "-r",
        "--replication_limit",
        type=int,
        default=3,
        help="maximum number of times that you want data to be replicated",
    )
    parser.add_argument(
        "-d",
        "--deletion_threshold",
        type=int,
        default=5,
        help="deletion threshold for data in cache",
    )
    parser.add_argument(
        "-k",
        "--signkey",
        type=str,
        default=conf["SIGNKEY"],
        help="sign key for communication",
    )

    args = parser.parse_args()

    main(args)
