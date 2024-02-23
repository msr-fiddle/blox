import os
import sys
import json
import grpc
import numa
import logging
import subprocess
import re
from concurrent import futures

from typing import Optional

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
import rm_pb2
import rm_pb2_grpc


class NodeManagerComm(object):
    """
    Node Manager communication class
    """

    def __init__(self, ipaddr: str, central_scheduler_port: int) -> None:
        """
        Initializes Node Manager Communication module.
        Args:
         ipaddr: IP-address and the port for resource managers GRPC server.
                 Format - ip:port
        """
        self.ipaddr = f"{ipaddr}:{central_scheduler_port}"
        self.ip_extract = re.compile(".?inet ([0-9.]+)")
        self.memory_extract = re.compile(".?MemAvailable:\s+([0-9]+)")

    def register_with_scheduler(
        self, interface: Optional[str] = None, nmipaddr: Optional[str] = None
    ) -> bool:
        """
        Register with worker based on calculated statistics
        Args:
            interface: The interface whose IP address we intend to use for
                       connecting to node manager.
            nmipaddr: Node Manager ipaddress to use
        Returns:
            bool
        Raises:
            ??
        """
        if nmipaddr == None and interface == None:
            raise AssertionError("Both IP address and enterface can not be none")

        if nmipaddr is not None:
            ipaddr = nmipaddr
        else:
            # getting IP address from an interface
            out = (
                subprocess.run(
                    f"ip -f inet addr show {interface}",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True,
                    shell=True,
                )
                .stdout.decode("utf-8")
                .strip()
            )
            ipaddr = self.ip_extract.findall(out)[0]
        # getting number of GPUs
        if os.path.isdir("/proc/driver/nvidia"):
            numgpus = (
                subprocess.run(
                    "nvidia-smi --query-gpu=name --format=csv,noheader | wc -l",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True,
                    shell=True,
                )
                .stdout.decode("utf-8")
                .strip()
            )
            gpuuuids = process = (
                subprocess.run(
                    "nvidia-smi -L | awk '{print $NF}' | tr -d '[)]'",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True,
                    shell=True,
                )
                .stdout.decode("utf-8")
                .strip()
            )

        else:
            numgpus = str(0)
        # getting memory from meminfo file
        with open("/proc/meminfo", "r") as fin:
            memory_data = fin.read()

        memoryCapacity = self.memory_extract.findall(memory_data)[0]
        request_to_rm = rm_pb2.RegisterRequest()
        request_to_rm.ipaddr = ipaddr
        request_to_rm.numGPUs = int(numgpus)
        request_to_rm.memoryCapacity = int(memoryCapacity)
        request_to_rm.gpuUUIDs = gpuuuids
        # TODO: Eventually fix this to send the full list
        if numa.available():
            last_node = 0
            for node in range(numa.get_max_node()):
                temp_mapping = list(numa.node_to_cpus(node))[-1]
                request_to_rm.cpuMaping[node] = temp_mapping
                last_node = node
                num_cpu_cores = temp_mapping
            # copying the last know cpu core to numCPUcores
            # request_to_rm.numCPUcores = list(numa.node_to_cpus(last_node))[-1]
            request_to_rm.numCPUcores = num_cpu_cores
            request_to_rm.numaAvailable = True
        else:
            numCPUcores = (
                subprocess.run(
                    "getconf _NPROCESSORS_ONLN",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True,
                    shell=True,
                )
                .stdout.decode("utf-8")
                .strip()
            )
            request_to_rm.numCPUcores = int(numCPUcores)
            request_to_rm.numaAvailable = False
            # putting dummy value in the cpuMaping dictionary
            request_to_rm.cpuMaping[0] = 0
        # sending the data to insecure channel
        print(self.ipaddr)
        with grpc.insecure_channel(self.ipaddr) as channel:
            stub = rm_pb2_grpc.RMServerStub(channel)
            response = stub.RegisterWorker(request_to_rm)
            print(request_to_rm)
            print(response.value)


def run():
    nmc = NodeManagerComm("localhost")
    nmc.register_with_scheduler(interface="enp2s0f0")


if __name__ == "__main__":
    logging.basicConfig()
    run()
