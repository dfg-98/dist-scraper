import pickle
import queue
import time
from multiprocessing import Process, Queue
from socket import (
    AF_INET,
    SO_BROADCAST,
    SO_REUSEADDR,
    SOCK_DGRAM,
    SOL_SOCKET,
    socket,
    timeout,
)

import zmq

import messages
from sockets import CloudPickleContext, CloudPickleSocket, no_block_REQ

BROADCAST_PORT = 4142

from settings import get_config

conf = get_config()
localhost = "127.0.0.1"


def discover_peer(times, log):
    """
    Discover a router in the subnet by broadcast.
    It not works offline.
    """
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
    sock.settimeout(2)

    broadcastAddress = ("255.255.255.255", BROADCAST_PORT)
    magic = conf["magic"]
    message = magic + messages.LOGIN_MESSAGE
    peer = ""
    network = True

    for i in range(times):
        try:
            log.info("Discovering peers", "discover_peer")
            sock.sendto(message.encode(), broadcastAddress)

            log.debug("Waiting to receive", "discover_peer")
            data, server = sock.recvfrom(4096)
            header, address = pickle.loads(data)
            if header.startswith(conf["magic"]):
                header = header.replace(conf["magic"], "")

                if header == "WELCOME":
                    log.info(f"Received confirmation: {address}", "discover_peer")
                    log.info(f"Server: {str(server)}", "discover_peer")
                    peer = f"{server[0]}:{address[1]}"
                    sock.close()
                    return peer, network
                else:
                    log.info("Login failed, retrying...", "discover_peer")
        except timeout as e:
            log.error("Socket " + str(e), "discover_peer")
        except Exception as e:
            log.error(e, "discover_peer")
            log.error(
                f"Connect to a network please, retrying connection in {(i + 1) * 2} seconds...",
                "discover_peer",
            )
            network = False
            # Factor can be changed
            time.sleep((i + 1) * 2)

    sock.close()

    return peer, network


def get_masters(master, discover_peer, address, login, q, log, signkey=None):
    """
    Request the list of master nodes to a active master, if <master> is not active, then try to discover a master active in the network.
    """
    context = CloudPickleContext()
    sock: CloudPickleSocket = no_block_REQ(context, timeout=1200)
    sock.connect(f"tcp://{master}")

    for i in range(4, 0, -1):
        try:
            sock.send_data(messages.GET_MASTERS, signkey=signkey)
            masters = sock.recv_data(signkey=signkey)
            log.info(f"Received masters: {masters}", "Get Masters")
            if login:
                sock.send_data((messages.NEW_MASTER, address), signkey=signkey)
                sock.recv_data(signkey=signkey)
            sock.close()
            q.put(master)
            break
        except zmq.error.Again as e:
            log.debug(e, "Get Master")
            master, _ = discover_peer(i, log)
            if master != "":
                sock.connect(f"tcp://{master}")
        except Exception as e:
            log.error(e, "Get Master")
        finally:
            if i == 1:
                q.put({})


def ping(master, q, time, log, signkey=None):
    """
    Process that make ping to a master.
    """
    context = zmq.Context()
    socket: CloudPickleSocket = no_block_REQ(context, timeout=time)
    socket.connect(f"tcp://{master[0]}:{master[1]}")
    status = True

    log.debug(f"PING to {master[0]}:{master[1]}", "Ping")
    try:
        socket.send_data((messages.PING,), signkey=signkey)

        msg = socket.recv_data(signkey=signkey)
        log.info(f"Received {msg} from {master[0]}:{master[1]} after ping", "Ping")
    except zmq.error.Again as e:
        log.debug(f"PING failed -- {e}", "Ping")
        status = False
    q.put(status)


def find_masters(
    masters,
    peer_queues,
    dead_queues,
    log,
    timeout=1000,
    sleep_time=15,
    master_from_input=None,
    signkey=None,
):
    """
    Process that ask to a seed for his list of seeds.
    """
    time.sleep(sleep_time)
    while True:
        # random address
        master = (localhost, 9999)
        data = list(masters)
        for s in data:
            # This process is useful to know if a seed is dead too
            ping_queue = Queue()
            process_ping = Process(
                target=ping,
                name="Ping",
                args=(s, ping_queue, timeout, log),
                kwargs={
                    "signkey": signkey,
                },
            )
            process_ping.start()
            status = ping_queue.get()
            process_ping.terminate()
            if not status:
                for q in dead_queues:
                    q.put(s)
                masters.remove(s)
            else:
                master = s
        masters_queue = Queue()
        process_get_masters = Process(
            target=get_masters,
            name="Get Masters",
            args=(
                f"{master[0]}:{master[1]}",
                discover_peer,
                None,
                False,
                masters_queue,
                log,
            ),
            kwargs={
                "signkey": signkey,
            },
        )
        log.debug("Finding new masters to pull from...", "Find Masters")
        process_get_masters.start()
        tmp = set(masters_queue.get())
        process_get_masters.terminate()
        # If Get Masters succeds to connect to a master
        if len(tmp) != 0:
            dif = tmp - masters
            if not len(dif):
                log.debug("No new master nodes where finded", "Find Masters")
            else:
                log.debug("New master nodes where finded", "Find Masters")

            for s in dif:
                for q in peer_queues:
                    q.put(s)
            masters.update(tmp)

            if master_from_input is not None:
                try:
                    masters.add(master_from_input.get(block=False))
                except queue.Empty:
                    pass

        # The amount of the sleep in production can be changed
        time.sleep(sleep_time)
