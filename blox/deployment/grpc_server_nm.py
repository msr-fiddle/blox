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
        # if use_redis:
        # configuring local data store with redis
        self.local_data_store = DataRelay(
            use_redis=use_redis, redis_host=redis_host, redis_port=redis_port
        )

        # Will keep job terminate ids in this list
        # else:
        # # configuring local data store with python dict
        # self.local_data_store = DataRelay()

    def ensure_terminate_status(self):
        """
        Check if all jobs in the terminate list have terminated before finishing the launch.
        """
        print("Ensuring previous round has terminated")
        jid_to_test = self.local_data_store.get_job_ids_to_check_terminate()
        while jid_to_test is not None:
            job_status = self.local_data_store.get_job_status(jid_to_test)
            while job_status != "exit":
                time.sleep(1)
                job_status = self.local_data_store.get_job_status(jid_to_test)
            out_val = self.local_data_store.push_terminated_jobs(jid_to_test)
            jid_to_test = self.local_data_store.get_job_ids_to_check_terminate()
        # all workers are done with checking if each individual job has finished.

        # now we need to make sure all jobs have been finished
        # this involves combining two lists

        # terminated_job_lists = self.local_data_store.get_terminated_jobs()
        # all_job_to_terminate = self.local_data_store.get_jobs_to_terminate()

        # time to check if elements are all there
        all_terminated = False

        while not all_terminated:
            all_terminated = True
            jid_to_test = self.local_data_store.get_job_ids_to_check_terminate()
            while jid_to_test is not None:
                job_status = self.local_data_store.get_job_status(jid_to_test)
                while job_status != "exit":
                    time.sleep(1)
                    job_status = self.local_data_store.get_job_status(jid_to_test)
                out_val = self.local_data_store.push_terminated_jobs(jid_to_test)
                jid_to_test = self.local_data_store.get_job_ids_to_check_terminate()
            terminated_job_lists = self.local_data_store.get_terminated_jobs()
            all_job_to_terminate = self.local_data_store.get_jobs_to_terminate()
            print("Terminated job list {}".format(terminated_job_list))
            print("All jobs to terminate {}".format(all_job_to_terminate))
            for terminate_id in all_job_to_terminate:
                if terminate_id not in terminated_job_lists:
                    all_terminated = False
                    time.sleep(1)
        print("All jobs terminated")

        return None

        # if len(self.job_terminate_ids) > 0:
        # while len(self.job_terminate_ids) > 0:
        # jid_to_test = self.job_terminate_ids.pop()
        # job_status = self.local_data_store.get_job_status(jid_to_test)
        # while job_status != "exit":
        # time.sleep(1)
        # job_status = self.local_data_store.get_job_status(jid_to_test)

    def LaunchJob(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Receives information for launching jobs on the node manager
        """
        # TODO: Add launching from docker
        print("In Launch Job")
        received_job = json.loads(request.response)
        command_to_run = received_job["launch_command"]
        launch_params = received_job["launch_params"]
        local_gpu_id = received_job["local_GPU_ID"]
        # resume_iter = received_job["resume_iter"]
        resume_iter = 0
        job_id = received_job["job_id"]
        environment_variable_pairs = received_job["env_variables"]

        # check if all previous jobs have terminated
        self.ensure_terminate_status()
        self.local_data_store.set_lease_status(received_job["job_id"], True)
        self.local_data_store.set_lease_status_rank0(received_job["job_id"], [], True)
        self.local_data_store.set_job_status(received_job["job_id"], "running")
        # os.environ["BLOX_JOB_ID"] = str(received_job["job_id"])
        # os.environ["GPU_ID"] = str(received_job["local_GPU_ID"])
        # os.environ["BLOX_LOG_DIR"] = "temp"
        # os.environ["SHOULD_RESUME"] = str(received_job["should_resume"])
        # os.environ["START_ITER"] = "0"
        # BloxIterator saves the checkpoint in the format {jobid_iternum.ckpt}
        # TODO: Support Model Parallel/Pipeline Parallel job checkpoints
        print("Launching Command")
        print(
            f"{command_to_run}  {' '.join(str(i) for i in launch_params)}  2>&1 | tee /dev/shm/job_{job_id}_local_gpu_{local_gpu_id}.log"
        )
        print("Environment variable pair {}".format(environment_variable_pairs))
        proc = subprocess.Popen(
            f"{command_to_run}  {' '.join(str(i) for i in launch_params)}  2>&1 | tee /dev/shm/job_{job_id}_local_gpu_{local_gpu_id}.log",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            shell=True,
            env=environment_variable_pairs,
        )
        # with open("job_file_db.txt", "w") as fopen:
        # fopen.write(
        # f"{command_to_run}  {' '.join(str(i) for i in launch_params)}  2>&1 | tee /dev/shm/job_{job_id}_local_gpu_{local_gpu_id}.log"
        # )
        # Debug code added
        # output, error = proc.communicate()
        # print(output, error)
        print(f"received_job {received_job}, node manager")
        return rm_pb2.BooleanResponse(value=True)

    def TerminateJob(self, request, context) -> rm_pb2.BooleanResponse:
        """
        This is called from Rank 0
        Terminate Job post launch. This will terminate lease. Which when read
        by blox iterator will terminate the job.
        This is has been called by the node manager
        """
        print("Called Terminate")
        all_job_ids_to_terminate = json.loads(request.response)["Job_ID_list"]
        all_corresponding_ip_address_to_terminate = json.loads(request.response)[
            "IP_addr_terminate"
        ]
        print("Terminate Jobs {}".format(all_job_ids_to_terminate))
        # self.job_terminate_ids.append(job_id_to_terminate)

        self.local_data_store.set_lease_status_rank0_batch_false(
            all_job_ids_to_terminate, all_corresponding_ip_address_to_terminate
        )
        # self.local_data_store.push_job_to_terminate(job_id_to_terminate)
        # self.local_data_store.set_lease_status_rank0(
        # job_id_to_terminate, ipaddr_to_terminate, False
        # )
        return rm_pb2.BooleanResponse(value=True)

    def TerminateJobfromPeer(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Terminate a job. Whose termination direction is recieved from peers
        Note: Every job is terminated by call from peer. This is called from Rank0
        """
        print("Called Terminate from Peer")
        job_id_to_terminate = json.loads(request.response)["Job_ID"]
        print("Terminate Job {}".format(job_id_to_terminate))
        # self.job_terminate_ids.append(job_id_to_terminate)
        # self.local_data_store.push_job_to_terminate(job_id_to_terminate)
        self.local_data_store.set_lease_status(job_id_to_terminate, False)
        return rm_pb2.BooleanResponse(value=True)

    def GetMetrics(self, request, context) -> rm_pb2.JsonResponse:
        """
        Return metrics as requested by resource manager
        """
        self.local_data_store.delete_all_job_terminate_list()
        received_job = json.loads(request.response)
        job_data = self.local_data_store.get_job_metrics(received_job["Job_ID"])
        # data_to_send = dict()
        # data_to_send[received_job["Job_ID"]] = job_data
        print("Got Job metrics")
        print("Job data {}".format(job_data))
        job_data_request = rm_pb2.JsonResponse()
        job_data_request.response = json.dumps(job_data)
        print("Json dump done")
        self.local_data_store.reset_job_metrics(received_job["Job_ID"])
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
        print(f"Set metrics {job_metrics})")
        previous_metrics = self.local_data_store.get_job_metrics(job_id)
        for key in job_metrics:
            if key == "attained_service":
                if key in previous_metrics:
                    job_metrics[key] += previous_metrics[key]
                else:
                    pass
            if key == "per_iter_time":
                if key in previous_metrics:
                    job_metrics[key] = (job_metrics[key] + previous_metrics[key]) / 2
                else:
                    pass
            if key == "iter_num":
                if key in previous_metrics:
                    job_metrics[key] += previous_metrics[key]
                else:
                    pass
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

    def NotifySuspend(self, request, context) -> rm_pb2.BooleanResponse:
        """
        Blox iterator notfies the node manager on successful exit.
        """
        jobs_to_terminate = request.value
        # TODO: Need to decide how to handel this
        self.local_data_store.set_job_metrics(jobs_to_terminate, {"job_suspend": True})
        return rm_pb2.BooleanResponse(value=True)

    # def SetMetricsFromRM(self, request, context) -> rm_pb2.BooleanResponse:
    # """
    # Set metrics pushed from resource manager.
    # Resource manager calls
    # """
    # recevied_data = json.loads(request.response)
    # job_id = recevied_data["Job_ID"]
    # job_metrics = recevied_data["metrics"]
    # self.local_data_store.set_rm_metrics(job_id, job_metrics)
    # return rm_pb2.BooleanResponse(value=True)

    # def GetMetricsFromRM(self, request, context) -> rm_pb2.JsonResponse:
    # """
    # Return the metrics pushed from the resource manager.
    # Application will call.
    # """
    # received_job = json.loads(request.response)
    # job_id = received_job["Job_ID"]
    # metric_to_fetch = received_job["metric"]
    # metric = self.local_data_store.get_rm_metrics(job_id, metric_to_fetch)
    # metric_response = rm_pb2.JsonResponse()
    # metric_response.response = json.dumps(metric)
    # return metric_response


def server(node_manager_port: int):
    """
    Node Manager Port
    node_manager_port (int): Node Manager Port
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
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
