import os
import sys
import json
import grpc
import logging
from typing import List
from concurrent import futures

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
sys.path.append(os.path.dirname(__file__))
print(sys.path)
import nm_pb2 as nm_pb2
import rm_pb2 as rm_pb2
import nm_pb2_grpc as nm_pb2_grpc
import redis
import node_data_relay


class BloxIteratorComm(object):
    """
    Class to connect to node manager
    """

    def __init__(
        self, jobid, node_manager_port=50052, redis_host="localhost", redis_port=6379
    ):
        self.node_manager_ip = f"localhost:{node_manager_port}"
        self.jobid = jobid
        # self.channel = grpc.insecure_channel(self.node_manager_ip)
        # self.redis_client = redis.Redis(
        # host="localhost", port=6379, decode_responses=True
        # )

        self.data_relay = node_data_relay.DataRelay(
            redis_host=redis_host, redis_port=redis_port
        )
        self.data_relay.reset_keys(self.jobid)
        self.job_launch_notify()
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
        lease_status = self.data_relay.get_lease_status(self.jobid, iteration)
        print("Job ID {}".format(lease_status))
        return lease_status

    def push_metrics(self, metrics: dict) -> bool:
        """
        Pushes metrics from blox iterator to node manager.
        Args:
            metrics : Key value store of metrics
        """
        # metrics_request = rm_pb2.JsonResponse()
        # metrics_request.response = json.dumps(
        # {"Job_ID": self.jobid, "metrics": metrics}
        # )
        # print(f"Input Metrics {metrics}")
        # # with grpc.insecure_channel(self.node_manager_ip) as channel:
        # stub = nm_pb2_grpc.NMServerStub(self.channel)
        # response = stub.SetMetrics(metrics_request)

        # NOTE: For now I am pushing Aggregation here
        # However, what will happen for aggregation function mechanisms in future.
        # Before this REDIS change we were fine with using additional
        previous_metrics = self.data_relay.get_job_metrics(self.jobid)
        print(f"Previous Metrics {previous_metrics}")
        #### Metrics Aggregation
        for key in metrics:
            if key == "attained_service":
                if key in previous_metrics:
                    metrics[key] += previous_metrics[key]
                else:
                    pass
            if key == "per_iter_time":
                if key in previous_metrics:
                    metrics[key] = (metrics[key] + previous_metrics[key]) / 2
                else:
                    pass
            if key == "iter_num":
                if key in previous_metrics:
                    metrics[key] += previous_metrics[key]
                else:
                    pass
        self.data_relay.set_job_metrics(self.jobid, metrics)
        print("Updated Metrics {}".format(metrics))
        return True

    def job_exit_notify(self) -> bool:
        """
        Job decides to terminate but does decide to call it at termination
        """
        # notify_exit_id = rm_pb2.IntVal()
        # notify_exit_id.value = self.jobid
        # with grpc.insecure_channel(self.node_manager_ip) as channel:
        # stub = nm_pb2_grpc.NMServerStub(channel)
        # response = stub.NotifyTerminate(notify_exit_id)

        self.data_relay.set_rm_metrics(self.jobid, "exit")
        return None

    def job_launch_notify(self) -> None:
        """
        Job launch set key
        """

        self.data_relay.set_job_status(self.jobid, "running")
        return None
