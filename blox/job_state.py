import sys
import time
import copy
import grpc
import json
import logging
import argparse
import pandas as pd
import time
from concurrent import futures

from typing import Tuple, List
from schedulers.pollux_lib.job import *
from schedulers.pollux_lib.applications import *

from blox_manager import BloxManager


class JobState(object):
    """
    Tracks state of jobs and all the metadata associated with them
    """

    def __init__(self, args: argparse.ArgumentParser):
        """
        Keep Track of job state
        """
        # self.blr = blox_resource_manager
        # dict of active jobs
        self.active_jobs = dict()
        """
        active_jobs is a dict that maps jid -> job info, which itself is a dict that includes:
        "tracked_metrics": dict
            "pollux_metrics": job.Job() object for pollux job # XY added
            "per_iter_time": float - mean updated
            "attained_service": float - additively updated
            "iter num": int - additively updated
            "attained_service_scheduler": - updated using "round_duration" * "numGPUs"   
            "job_exit": optional(boolean)
        "time_since_scheduled": int
        "job_priority": int
        "previously_launched": boolean
        "is_running": boolean
        "suspended": int
        "simulation": boolean
        "running_ip_address": 
        "num_GPUs"  
        "submit_time"
        """
        # count number of accepted jobs
        self.job_counter = 0
        self.job_completion_stats = dict() # used when job finishes
        self.job_responsiveness_stats = dict()
        self.cluster_stats = dict()
        self.custom_metrics = dict()
        self.job_runtime_stats = dict()
        self.finished_job = dict()  # keys are ids of the jobs which have finished
        self.job_ids_to_track = list(range(args.start_id_track, args.stop_id_track + 1))
        self.time = 0
        self.scheduler_name = args.scheduler_name
        if self.scheduler_name == "Pollux":
            self.interference = args.interference
            self.round_duration = args.round_duration

    # def get_new_jobs(self):
    # """
    # Fetch any new jobs which have arrived at the scheduler
    # """
    # new_jobs = self.blr.rmserver.get_new_jobs()
    # return new_jobs

    # def get_new_jobs_sim(self):
    # """
    # Get new jobs for simulation
    # """
    # new_jobs = self.blr.rmserver.get_jobs_sim(self.simulator_time)
    # return new_jobs

    # def add(self, new_jobs: List[dict]):
    # """
    # Add new jobs
    # """
    # if len(new_jobs) > 0:
    # self._add_new_jobs(new_jobs)

    def update_metrics(self, metric_data: dict, round_duration: int) -> None:
        """
        Update the metrics fetched at end of each round duration
        """
        for jid in self.active_jobs:
            if self.active_jobs[jid]["is_running"] == True:
                if len(metric_data.get(jid)) > 0:
                    # replace only when we have got metrics
                    # add scheduler side metrics
                    # TODO: Good to have a separate function for this in future
                    if (
                        "attained_service_scheduler"
                        in self.active_jobs[jid]["tracked_metrics"]
                    ):
                        metric_data[jid][
                            "attained_service_scheduler"
                        ] = self.active_jobs[jid]["tracked_metrics"][
                            "attained_service_scheduler"
                        ] + (
                            round_duration * self.active_jobs[jid]["num_GPUs"]
                        )
                    else:
                        metric_data[jid]["attained_service_scheduler"] = round_duration
                    self.active_jobs[jid]["tracked_metrics"].update(
                        metric_data.get(jid)
                    )

                    # Mark Job completion
                    # if "iter_num" in self.active_jobs[jid]["tracked_metrics"]:
                    # num_iterations = self.active_jobs[jid]["tracked_metrics"][
                    # "iter_num"
                    # ]
                    # if (
                    # num_iterations
                    # >= self.active_jobs[jid]["job_total_iteration"]
                    # ):
                    # self.active_jobs[jid]["tracked_metrics"].update(
                    # {"job_exit": True}
                    # )
        return None

    def add_new_jobs(self, new_jobs: List[dict]) -> None:
        """
        Pop jobs from the new queue and assign to the dictionary

        new_jobs : list of new jobs submitted to resource manager
        """
        if len(new_jobs) > 0:
            while True:
                try:
                    jobs = new_jobs.pop(0)
                    # TODO: Make this more permanent
                    if "tracked_metrics" not in jobs:
                        # if not in job dict
                        params_to_track = ["per_iter_time", "attained_service"]
                        default_values_param = [0, 0]
                        tracking_dict = dict()
                        for p, v in zip(params_to_track, default_values_param):
                            tracking_dict[p] = v
                        jobs["tracked_metrics"] = tracking_dict

                    if self.scheduler_name == "Pollux":
                        """
                        Create pollux.job.Job object, decide how to refer to model name, the options of which include
                        "bert", "cifar10", "ncf", "imagenet", "deepspeech2", "yolov3"
                        """
                        print(jobs)
                        job_temp = Job(self.job_counter, APPLICATIONS[jobs["application"]],
                                       jobs["job_arrival_time"], self.time)
                        if job_temp.application.name == "ncf":
                            job_temp.target_batch_size = 32768
                        jobs["tracked_metrics"]["pollux_metrics"] = job_temp

                    jobs["time_since_scheduled"] = 0
                    jobs["job_priority"] = 999
                    jobs["previously_launched"] = False
                    self.active_jobs[self.job_counter] = jobs
                    self.active_jobs[self.job_counter]["is_running"] = False
                    self.job_counter += 1
                except IndexError:
                    # remove the job counter
                    break
