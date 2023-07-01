import json
import os
import re
import time
from multiprocessing import Process, Queue
from threading import Lock, Semaphore, Thread
from urllib.parse import urljoin
from uuid import uuid4

import zmq
from bs4 import BeautifulSoup
from scrapy.http import HtmlResponse

import messages
from logger import GREEN, LOGLVLMAP, RED, RESET, get_logger
from service_discovery import discover_peer, find_masters, get_masters
from sockets import CloudPickleContext, CloudPickleSocket, no_block_REQ
from utils import change_html

lock_socket_req = Lock()
counter_socket_req = Semaphore(value=0)

log = get_logger("Master")


def connect_to_masters(sock, peer_queue):
    """
    Thread that connect REQ socket to masters.
    """
    for addr, port in iter(peer_queue.get, messages.STOP):
        with lock_socket_req:
            log.debug(f"Connecting to master {addr}:{port}", "Connect to Masters")
            sock.connect(f"tcp://{addr}:{port}")
            counter_socket_req.release()
            log.info(
                f"Dispatcher connected to master with address:{addr}:{port})",
                "Connect to Masters",
            )


def disconnect_to_masters(sock, peer_queue):
    """
    Thread that disconnect REQ socket to masters.
    """
    for addr, port in iter(peer_queue.get, messages.STOP):
        with lock_socket_req:
            log.debug(f"Disconnecting to master {addr}:{port}", "Disconnect to Masters")
            sock.disconnect(f"tcp://{addr}:{port}")
            counter_socket_req.acquire()
            log.info(
                f"Dispatcher disconnected to master with address:{addr}:{port})",
                "Disconnect to Masters",
            )


def get_master_from_file(peer_queue, extra_queue):
    """
    Process that gets an address of a master node by standart input.
    """
    # Creating network.txt
    log.debug("Creating network.txt...", "Get master from file")
    new_file = open("network.txt", "w")
    new_file.close()

    while True:
        # get input
        with open("network.txt", "r") as f:
            s = f.read()
            if s == "":
                time.sleep(1)
                continue
            log.info(f'Get "{s}" from network.txt', "Get master from file")
        with open("network.txt", "w") as f:
            f.write("")

        # ip_address:port_number
        regex = re.compile("\d{,3}\.\d{,3}\.\d{,3}\.\d{,3}:\d+")
        try:
            assert regex.match(s).end() == len(s)
            addr, port = s.split(":")
            peer_queue.put((addr, int(port)))
            extra_queue.put((addr, int(port)))
        except (AssertionError, AttributeError):
            log.error(
                f"Parameter master inserted is not a valid ip_address:port_number",
                "Get master from file",
            )


def writer(root, url, old, data, name, graph):
    """
    Write all the files on which <url> depends
    on the <root> folder, taking their contents
    from <data> and its name from <name>.
    The <graph> dependency tree is traversed while
    there are dependencies that are not found in <old>
    """
    if url in old or url not in data:
        return
    old.add(url)

    url_name = name[url]
    with open(f"{root}/{url_name}", "wb") as fd:
        fd.write(data[url])

    for next_url in graph[url]:
        writer(root, next_url, old, data, name, graph)


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

    def dispatch(self, queue):
        """
        Start to serve the Dispatcher.
        """
        context = CloudPickleContext()
        socket: CloudPickleSocket = no_block_REQ(context)

        masters_queue1 = Queue()
        masters_queue2 = Queue()
        for address in self.masters:
            masters_queue1.put(address)

        connection_thread = Thread(
            target=connect_to_masters,
            name="Connect to Masters",
            args=(socket, masters_queue1),
        )
        connection_thread.start()

        disconection_queue = Queue()
        disconnection_thread = Thread(
            target=disconnect_to_masters,
            name="Disconnect to Masters",
            args=(socket, disconection_queue),
        )
        disconnection_thread.start()

        find_masters_process = Process(
            target=find_masters,
            name="Find Masters",
            args=(
                set(self.seeds),
                [masters_queue1],
                [disconection_queue],
                log,
                2000,
                10,
                masters_queue2,
            ),
            kwargs={"signkey": self.signkey},
        )
        find_masters_process.start()

        input_process = Process(
            target=get_master_from_file,
            name="Get master from file",
            args=(masters_queue1, masters_queue2),
        )
        input_process.start()

        graph = {}
        depth = 1
        data = {}
        url_mapper = {url: f"url_{i}" for i, url in enumerate(self.urls)}

        src = set()
        while True:
            new_data = {}
            while len(self.urls):
                try:
                    url = self.urls[0]
                    self.urls.pop(0)
                    self.urls.append(url)
                    with counter_socket_req:
                        socket.send_data(
                            (messages.URL, self.uuid, url), signkey=self.signkey
                        )

                        log.debug(f"Send {url}", "dispatch")
                        response = socket.recv_data(signkey=self.signkey)
                    assert isinstance(
                        response, tuple
                    ), f"Bad response, expected <tuple> find {type(response)}"
                    assert len(response) == 2, "bad response size"
                    assert (
                        response[0] == messages.RESPONSE
                    ), "Unexpected response format"
                    _, package = response
                    log.debug(
                        f"Received a package with size: {len(package)}", "dispatch"
                    )
                    for recv_url, html in package.items():
                        try:
                            idx = self.urls.index(recv_url)
                            log.info(f"{recv_url} {GREEN}OK{RESET}", "dispatch")
                            new_data[recv_url] = html
                            self.urls.pop(idx)
                        except ValueError:
                            log.debug(f"Unnecesary {recv_url}", "dispatch")
                except AssertionError as e:
                    log.error(e, "dispatch")
                except zmq.error.Again as e:
                    log.debug(e, "dispatch")
                except Exception as e:
                    log.error(e, "dispatch")
                time.sleep(0.8)

            log.info(f"Depth {depth} done", "dispatch")
            for url, html in new_data.items():
                graph[url] = set()
                try:
                    text = html.decode()
                    soup = BeautifulSoup(html, "html.pazrser")
                    tags = soup.find_all(
                        lambda tag: tag.has_attr("href") or tag.has_attr("src")
                    )
                    new_urls = [["src", "href"][tag.has_attr("href")] for tag in tags]
                    changes = []
                    for i, attr in enumerate(new_urls):
                        url_dir = urljoin(url, tags[i][attr])
                        graph[url].add(url_dir)
                        if url_dir not in url_mapper:
                            url_mapper[url_dir] = f"url_{len(url_mapper)}"
                        changes.append((tags[i][attr], url_mapper[url_dir]))
                        if attr == "src" or tags[i].name == "link":
                            src.add(url_dir)
                            continue
                        self.urls.append(url_dir)
                    html = change_html(text, changes).encode()
                except UnicodeDecodeError:
                    log.debug(f"{url} is not decodeable", "dispatch")
                except:  # BeautifulSoup strange exceptions related with his's logger
                    pass
                new_data[url] = html
            data.update(new_data)
            self.urls = set(self.urls)
            self.urls.difference_update(self.old)
            self.old.update(self.urls)
            self.urls = list(self.urls)

            if depth > self.depth:
                break
            if depth == self.depth:
                src.difference_update(self.old)
                self.old.update(src)
                self.urls = list(src)
            depth += 1
            log.info(
                f"Number of URLs to be requested for download: {RED}{len(self.urls)}{RESET}",
                "dispatch",
            )

        log.info(f"Starting to write data", "dispatch")
        for i, url in enumerate(self.originals):
            try:
                res = HtmlResponse(url=url, body=data[url], encoding="utf8")
                base = res.css("title::text")[0].get()
            except:
                base = f"web_page_{i}"
            try:
                os.makedirs(f"Downloads/{base}-data")
            except:
                pass
            writer(f"downloads/{base}-data", url, set(), data, url_mapper, graph)

            html = data[url]
            if len(graph[url]) > 0:
                text = data[url].decode()
                changes = []
                for dep in graph[url]:
                    name = url_mapper[dep]
                    changes.append((name, f"{base}-data/{name}"))
                html = change_html(text, changes).encode()
            with open(f"Downloads/{base}", "wb") as fd:
                fd.write(html)

        log.info(
            f"Dispatcher:{self.uuid} has completed his URLs succefully", "dispatch"
        )
        log.debug(f"Dispatcher:{self.uuid} disconnecting from system", "dispatch")
        # disconnect

        queue.put(True)
        find_masters_process.terminate()
        input_process.terminate()


def main(args):
    log.setLevel(LOGLVLMAP[args.level])

    urls = []
    try:
        assert os.path.exists(args.urls), "No URLs to request"
        with open(args.urls, "r") as fd:
            urls = json.load(fd)
    except Exception as e:
        log.error(e, "main")

    log.info(urls, "main")
    uuid = str(uuid4())
    d = Dispatcher(urls, uuid, args.address, args.port, args.depth)
    master = args.master

    while not d.login(master):
        log.info(
            "Enter an address of an existing master node. Insert as ip_address:port_number. Press ENTER if you want to omit this address. Press q if you want to exit the program"
        )
        master = input("-->")
        if master == "":
            continue
        master = master.split()[0]
        if master == "q":
            break

    if master != "q":
        terminate_queue = Queue()
        dispatch_process = Process(target=d.dispatch, args=(terminate_queue,))
        dispatch_process.start()
        terminate_queue.get()
        log.info(f"Dispatcher:{uuid} finish!!!", "main")
        dispatch_process.terminate()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Client of a distibuted scrapper")
    parser.add_argument(
        "-a", "--address", type=str, default="127.0.0.1", help="node address"
    )
    parser.add_argument("-p", "--port", type=int, default=4142, help="connection port")
    parser.add_argument("-l", "--level", type=str, default="INFO", help="log level")
    parser.add_argument(
        "-d", "--depth", type=int, default=1, help="depth of recursive downloads"
    )
    parser.add_argument(
        "-u",
        "--urls",
        type=str,
        default="urls",
        help="path of file that contains the urls set",
    )
    parser.add_argument(
        "-m",
        "--master",
        type=str,
        default=None,
        help="address of an existing master node. Insert as ip_address:port_number",
    )

    args = parser.parse_args()

    main(args)
