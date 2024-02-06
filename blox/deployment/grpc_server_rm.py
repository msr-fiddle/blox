import os
import sys
import time
import queue
import json
import grpc
import copy
import logging
import pandas as pd
from concurrent import futures

from typing import Tuple, List

# sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
# sys.path.append(os.path.join(os.path.dirname(__file__))
print(sys.path)
import rm_pb2 as rm_pb2
import rm_pb2_grpc as rm_pb2_grpc
import simulator_pb2 as sim_pb2
import simulator_pb2_grpc as sim_pb2_grpc

from google.protobuf.json_format import MessageToDict


class RMServer(rm_pb2_grpc.RMServerServicer):
    def __init__(self, simulator_rpc_port):
        # new servers added
        self.added_servers = queue.Queue()
        # new jobs submitted
        self.new_jobs = list()
        # time at resource manager
        self.time_rm_queue = queue.Queue()
        self.simulator_rpc_port = simulator_rpc_port

    def RegisterWorker(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Accepts the information from the node manager.
        This registers the workers and the information we need
        """
        # print("Called R worker")
        message_processed = MessageToDict(request, including_default_value_fields=True)
        # TODO: Put a lock here
        self.added_servers.put(message_processed)
        # print("Length of added server queue {}".format(self.added_servers.qsize()))
        return rm_pb2.BooleanResponse(value=True)

    def AcceptJob(self, request, context):
        """
        Accepts a new job to run from
        """
        received_job = json.loads(request.response)
        received_job["submit_time"] = time.time()
        params_to_track = received_job["params_to_track"]
        default_values_param = received_job["default_values"]
        tracking_dict = dict()
        for p, v in zip(params_to_track, default_values_param):
            tracking_dict[p] = v
        received_job["tracked_metrics"] = tracking_dict
        # received_job["time_since_scheduled"] = 0
        # received_job["job_priority"] = 999
        # TODO: Put a lock here
        self.new_jobs.append(received_job)
        return rm_pb2.BooleanResponse(value=True)

    def ReturnTime(self, request, context):
        """
        Returns the current time at which resource manager is
        """
        # this will block till the time is updated
        val = self.time_rm_queue.get()
        return rm_pb2.IntVal(value=val)

    # def get_state(self) -> Tuple[List[dict], List[dict]]:
    # """
    # Threadsafe copy of active jobs and server map
    # # NOTE: This is old version version which returns both new jobs andnew nodes
    # """
    # # TODO: Put a lock here
    # new_jobs_copy = copy.deepcopy(self.new_jobs)
    # added_servers_copy = copy.deepcopy(self.added_servers)
    # self.new_jobs = list()
    # self.added_servers = list()
    # return (new_jobs_copy, added_servers_copy)

    def get_new_jobs(self) -> List[dict]:
        """
        Threadsafe copy of new jobs
        """
        # print("In get new jobs")
        # TODO: Put a lock here
        new_jobs_copy = copy.deepcopy(self.new_jobs)
        self.new_jobs = list()
        # print("After got new jobs")
        return new_jobs_copy

    def get_new_nodes(self) -> List[dict]:
        """
        Threadsafe copy of active jobs and server map
        # NOTE: This is old version version which returns both new jobs andnew nodes
        """
        # TODO: Put a lock here
        # added_servers_copy = copy.deepcopy(self.added_servers)
        # self.added_servers = list()
        added_servers_copy = list()
        while self.added_servers.qsize() > 0:
            added_servers_copy.append(self.added_servers.get())
        # print("Length of added servers {}".format(len(added_servers_copy)))
        return added_servers_copy

    def get_new_sim_config(self):
        """
        When performing simulation get the next config to initialize the
        simulator
        """
        dummy_val = rm_pb2.IntVal()
        dummy_val.value = 10
        with grpc.insecure_channel(f"localhost:{self.simulator_rpc_port}") as channel:
            stub = sim_pb2_grpc.SimServerStub(channel)
            response = stub.GetConfig(dummy_val)
        new_config = json.loads(response.response)
        return new_config

    def get_jobs_sim(
        self,
        simulator_time,
    ) -> List[dict]:
        """
        Get jobs when running the simulator
        """
        sim_time = rm_pb2.IntVal()
        sim_time.value = simulator_time
        # the training starts with
        options = [("grpc.max_receive_message_length", 10 * 1024 * 1024)]
        # print("Called rpc server")
        with grpc.insecure_channel(
            f"127.0.0.1:{self.simulator_rpc_port}", options=options
        ) as channel:
            stub = sim_pb2_grpc.SimServerStub(channel)
            response = stub.GetJobs(sim_time)
        # print("Rpc server return")
        jobs_to_run = json.loads(response.response)
        job_list = list(jobs_to_run.values())
        # for j in job_list:
        # params_to_track = j["params_to_track"]
        # default_values_param = j["default_values"]
        # tracking_dict = dict()
        # for p, v in zip(params_to_track, default_values_param):
        # tracking_dict[p] = v
        # j["tracked_metrics"] = tracking_dict
        # j["time_since_scheduled"] = 0
        # j["job_priority"] = 999
        return job_list


def server(rm_server_rpc_port: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    rm_pb2_grpc.add_RMServerServicer_to_server(RMServer(), server)
    server.add_insecure_port(f"[::]:{rm_server_rpc_port}")
    server.start()
    server.wait_for_termination()


def start_server(rmserver: RMServer, rm_server_rpc_port: int) -> grpc.server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    rm_pb2_grpc.add_RMServerServicer_to_server(rmserver, server)
    server.add_insecure_port(f"[::]:{rm_server_rpc_port}")
    server.start()
    return server


if __name__ == "__main__":
    logging.basicConfig()
    server()
