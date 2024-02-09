import time
import copy
import grpc
import argparse
import pandas as pd
from concurrent import futures

import blox.deployment.grpc_server_nm as nm_serve
import blox.deployment.grpc_client_nm as nm_client

from typing import Tuple
import redis


class NodeManagerMain(object):
    """
    Main node manager class
    """

    def __init__(self, ipaddr: str, central_scheduler_port: int) -> None:
        """
        Initializes node manager main class
        Args:
        ipaddr: IPaddress of the central scheduler
        central_scheduler_rpc_port: Central Scheduler RPC port
        """
        self.ipaddr = ipaddr
        self.node_manager_comm = nm_client.NodeManagerComm(
            self.ipaddr, central_scheduler_port
        )
        # flush db at launch
        self.redis_client = redis.Redis(
            host="localhost", port=6379, decode_responses=True
        )
        self.redis_client.flushdb()

    def register_with_scheduler(self, interface: str) -> None:
        """
        Register the node with the central scheduler
        """
        self.node_manager_comm.register_with_scheduler(interface)
        # TODO: Add a retry loop to connect to central schedule


def parse_args(parser: argparse.ArgumentParser) -> argparse.PARSER:
    """
    Parses the arguments for node manager
    Args:
        parser: Parses argument parser
    Return:
        args: Parsed arguments
    """
    parser.add_argument(
        "--ipaddr", required=True, type=str, help="IP of the central scheduler"
    )
    parser.add_argument(
        "--interface", type=str, help="The interface to get the ipaddr from"
    )

    parser.add_argument(
        "--node-manager-port",
        default=50052,
        type=int,
        help="Node Manager RPC Port, should be same as specified as specified in the central scheduler",
    )

    parser.add_argument(
        "--central-scheduler-port",
        default=50051,
        type=int,
        help="Central Scheduler Port, should be same as specified in the central scheduler port",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Used to push data for debuging",
    )

    parser.add_argument("--ipaddr-self", type=str, help="IP address of self")
    parser.add_argument(
        "--use-redis",
        action="store_true",
        default=False,
        help="Use redis as data store",
    )
    parser.add_argument(
        "--redis-host", default="localhost", help="Location of redis server"
    )

    parser.add_argument("--redis-port", default=6379, help="Redis port to connect")
    args = parser.parse_args()

    return args


def launch_server(args) -> Tuple[grpc.Server, nm_serve.NMServer]:
    """
    Lauches the GRPC server and returns the server object
    Args:
        None
    Returns:
        server: GRPC server object
        nmserver: The class object to work with rmserver
    """
    nmserver = nm_serve.NMServer(args.use_redis, args.redis_host, args.redis_port)
    server = nm_serve.start_server(nmserver, args.node_manager_port)
    return (server, nmserver)


def main(args):
    """
    This is the control loop of running the server.
    1. It initializes the node manager server
    2. Registers the node with resource manager
    """
    server, nmserver = launch_server(args)
    node_manager_main = NodeManagerMain(args.ipaddr, args.central_scheduler_port)
    node_manager_main.register_with_scheduler(args.interface)
    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.stop(0)
        print("Exit by ctrl c")


if __name__ == "__main__":
    args = parse_args(
        argparse.ArgumentParser(description="Arguments for starting the node manager")
    )
    main(args)
