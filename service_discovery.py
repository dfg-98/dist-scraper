import pickle
import time
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

from .sockets import CloudPickleContext, CloudPickleSocket, no_block_REQ

BROADCAST_PORT = 4142

from settings import get_config

conf = get_config()


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
            log.info("Discovering peers", "discoverPeer")
            sock.sendto(message.encode(), broadcastAddress)

            log.debug("Waiting to receive", "discoverPeer")
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
                sock.send_data((messages.NEW_MASTER, address))
                sock.recv_data()
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
