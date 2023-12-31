import os
import sys
import time
import json
import grpc
import copy
import logging
import subprocess
import pandas as pd
from concurrent import futures

from typing import Tuple

# sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
sys.path.append(os.path.dirname(__file__))
print(sys.path)
import nm_pb2 as nm_pb2
import rm_pb2 as rm_pb2
import nm_pb2_grpc as nm_pb2_grpc

from node_data_relay import DataRelay

from google.protobuf.json_format import MessageToDict


class NMServer(nm_pb2_grpc.NMServerServicer):
    def __init__(self, use_redis, redis_host, redis_port):
        self.job_mapping = dict()  # Job ID to GPU mapping
        if use_redis:
            # configuring local data store with redis
            self.local_data_store = DataRelay(
                use_redis=use_redis, redis_host=redis_host, redis_port=redis_port
            )
        else:
            # configuring local data store with python dict
            self.local_data_store = DataRelay()

    def LaunchJob(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Receives information for launching jobs on the node manager
        """
        # TODO: Add launching from docker
        received_job = json.loads(request.response)
        command_to_run = received_job["launch_command"]
        local_gpu_id = received_job["local_GPU_ID"]
        resume_iter = received_job["resume_iter"]
        job_id = received_job["job_id"]
        self.local_data_store.set_lease_status(received_job["job_id"], True)
        os.environ["BLOX_JOB_ID"] = str(received_job["job_id"])
        os.environ["GPU_ID"] = str(received_job["local_GPU_ID"])
        os.environ["BLOX_LOG_DIR"] = "temp"
        os.environ["SHOULD_RESUME"] = str(received_job["should_resume"])
        os.environ["START_ITER"] = "0"
        # BloxIterator saves the checkpoint in the format {jobid_iternum.ckpt}
        # TODO: Support Model Parallel/Pipeline Parallel job checkpoints
        proc = subprocess.Popen(
            f"{command_to_run} --local_gpu_id {local_gpu_id} --jid {job_id} --resume_iter {resume_iter}",
            stdout=subprocess.PIPE,
            # stderr=subprocess.STDOUT,
            shell=True,
        )
        # Debug code added
        # output, error = proc.communicate()
        # print(output, error)
        print(f"received_job {received_job}, node manager")
        return rm_pb2.BooleanResponse(value=True)

    def TerminateJob(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Terminate Job post launch. This will terminate lease. Which when read
        by blox iterator will terminate the job.
        """
        job_id_to_terminate = json.loads(request.response)["Job_ID"]
        self.local_data_store.set_lease_status(job_id_to_terminate, False)
        return rm_pb2.BooleanResponse(value=True)

    def GetMetrics(self, request, context) -> rm_pb2.JsonResponse:
        """
        Return metrics as requested by resource manager
        """
        received_job = json.loads(request.response)
        job_data = self.local_data_store.get_job_metrics(received_job["Job_ID"])
        # data_to_send = dict()
        # data_to_send[received_job["Job_ID"]] = job_data
        job_data_request = rm_pb2.JsonResponse()
        job_data_request.response = json.dumps(job_data)
        return job_data_request

    def GetLease(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Return lease status. Called by blox iterator.
        Also writes out the current iteration number
        """
        # TODO: Put lock
        received_job = json.loads(request.response)
        job_id = received_job["Job_ID"]
        iteration_number = received_job["Iteration"]
        # print(f"{job_id},{iteration_number} server nm")
        lease = self.local_data_store.get_lease_status(job_id, iteration_number)

        return rm_pb2.BooleanResponse(value=lease)

    def SetMetrics(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Push metrics from blox iterator
        """
        # TODO: Add parsing scripts when adding metrics

        recevied_data = json.loads(request.response)
        job_id = recevied_data["Job_ID"]
        job_metrics = recevied_data["metrics"]
        # print(f"Set metrics {job_metrics})")
        self.local_data_store.set_job_metrics(job_id, job_metrics)
        return rm_pb2.BooleanResponse(value=True)

    def NotifyTerminate(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Blox iterator notfies the node manager on successful exit.
        """
        jobs_to_terminate = request.value
        # TODO: Need to decide how to handel this
        self.local_data_store.set_job_metrics(jobs_to_terminate, {"job_exit": True})
        return rm_pb2.BooleanResponse(value=True)

    def SetMetricsFromRM(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Set metrics pushed from resource manager.
        Resource manager calls
        """
        recevied_data = json.loads(request.response)
        job_id = recevied_data["Job_ID"]
        job_metrics = recevied_data["metrics"]
        self.local_data_store.set_rm_metrics(job_id, job_metrics)
        return rm_pb2.BooleanResponse(value=True)

    def GetMetricsFromRM(self, request, context) -> rm_pb2.JsonResponse:
        """
        Return the metrics pushed from the resource manager.
        Application will call.
        """
        received_job = json.loads(request.response)
        job_id = received_job["Job_ID"]
        metric_to_fetch = received_job["metric"]
        metric = self.local_data_store.get_rm_metrics(job_id, metric_to_fetch)
        metric_response = rm_pb2.JsonResponse()
        metric_response.response = json.dumps(metric)
        return metric_response


def server(node_manager_port: int):
    """
    Node Manager Port
    node_manager_port (int): Node Manager Port
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    rm_pb2_grpc.add_RMServerServicer_to_server(NMServer(), server)
    server.add_insecure_port(f"[::]:{node_manager_port}")
    server.start()
    server.wait_for_termination()


def start_server(nmserver: NMServer, node_manager_port: int) -> grpc.server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    nm_pb2_grpc.add_NMServerServicer_to_server(nmserver, server)
    server.add_insecure_port(f"[::]:{node_manager_port}")
    server.start()
    return server
