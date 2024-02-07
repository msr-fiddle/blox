import os
import sys
import json
import grpc
import logging
from typing import List
from concurrent import futures

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
print(sys.path)
import nm_pb2 as nm_pb2
import rm_pb2 as rm_pb2
import nm_pb2_grpc as nm_pb2_grpc


class BloxIteratorComm(object):
    """
    Class to connect to node manager
    """

    def __init__(self, jobid, node_manager_port=50052):
        self.node_manager_ip = f"localhost:{node_manager_port}"
        self.jobid = jobid
        self.channel = grpc.insecure_channel(self.node_manager_ip)
        return None

    def check_lease(self, iteration: int) -> bool:
        """
        For a given job_id check job lease. Also writes out the iteration
        number for logging.
        Args:
            None
        Return:
            lease_status: True if the job still has lease
        """
        lease_request = rm_pb2.JsonResponse()

        lease_request.response = json.dumps(
            {"Job_ID": self.jobid, "Iteration": iteration}
        )
        # print("Lease Request respoi )
        # with grpc.insecure_channel(self.node_manager_ip) as channel:
        stub = nm_pb2_grpc.NMServerStub(self.channel)
        response = stub.GetLease(lease_request)
        return response.value

    def push_metrics(self, metrics: dict) -> bool:
        """
        Pushes metrics from blox iterator to node manager.
        Args:
            metrics : Key value store of metrics
        """
        metrics_request = rm_pb2.JsonResponse()
        metrics_request.response = json.dumps(
            {"Job_ID": self.jobid, "metrics": metrics}
        )
        print(f"Input Metrics {metrics}")
        # with grpc.insecure_channel(self.node_manager_ip) as channel:
        stub = nm_pb2_grpc.NMServerStub(self.channel)
        response = stub.SetMetrics(metrics_request)
        return response.value

    def job_exity_notify(self) -> bool:
        """
        Job decides to terminate but does decide to call it at termination
        """
        notify_exit_id = rm_pb2.IntVal()
        notify_exit_id.value = self.jobid
        with grpc.insecure_channel(self.node_manager_ip) as channel:
            stub = nm_pb2_grpc.NMServerStub(channel)
            response = stub.NotifyTerminate(notify_exit_id)
        return response.value

    def get_job_metrics_from_rm(self, metric_to_fetch: str) -> dict:
        """
        Job needs to pull metrics send by resource manager.
        Example in case of nexus need to get routing table for frontend and
        execution schedule for the backend
        """
        job_metrics_id = rm_pb2.JsonResponse()
        job_metrics_id.response = json.dumps(
            {"Job_ID": self.jobid, "metric": metric_to_fetch}
        )
        with grpc.insecure_channel(self.node_manager_ip) as channel:
            stub = nm_pb2_grpc.NMServerStub(channel)
            response = stub.GetMetricsFromRM(job_metrics_id)
        return json.loads(response.response)
