import copy
import json
import pandas as pd
import itertools
from typing import List

round_duration = 300


def get_metric_to_collect(
    pending_jobs,
    scheduler_choices,
    admission_policy,
    placement_policy,
    job_state,
    cluster_state,
    rounds_to_simulate,
    simulator_time,
    scheduler_choices_name,
    admission_choices_name,
    placement_choices_name,
    running_new_metric,
):
    """
    Job state creation.
    pending_jobs (All the pending jobs, including the ones in acceptance policy)

    simulator_time (current simulator time)
    """

    all_possible_combinations_idx = itertools.product(
        range(len(scheduler_choices)),
        range(len(admission_policy)),
        range(len(placement_policy)),
    )
    # combinations to run
    all_possible_combinations_idx = list(all_possible_combinations_idx)
    all_possible_combinations = [
        [
            copy.deepcopy(scheduler_choices[idx1]),
            copy.deepcopy(admission_policy[idx2]),
            copy.deepcopy(placement_policy[idx3]),
            copy.deepcopy(job_state),
            copy.deepcopy(cluster_state),
            copy.deepcopy(pending_jobs),
        ]
        for idx1, idx2, idx3 in all_possible_combinations_idx
    ]

    all_possible_names = [
        [
            scheduler_choices_name[idx1],
            admission_choices_name[idx2],
            placement_choices_name[idx3],
        ]
        for idx1, idx2, idx3 in all_possible_combinations_idx
    ]

    if len(running_new_metric) == 0:
        # it's empty and need to initialized
        for name in all_possible_names:
            running_new_metric["-".join(name)] = [0, 0]
    new_running_jct_dict = dict()
    for name in all_possible_names:
        new_running_jct_dict["-".join(name)] = [0, 0]
    # combination_dict_avg_jct = dict()
    for name_idx, combination_to_run in enumerate(all_possible_combinations):
        scheduling_policy = combination_to_run[0]
        admission_policy = combination_to_run[1]
        placement_policy = combination_to_run[2]
        job_state = combination_to_run[3]
        cluster_state = combination_to_run[4]
        pending_jobs = combination_to_run[5]

        current_name = all_possible_names[name_idx]
        # need a dictioary to track running jcts
        # import ipdb

        # ipdb.set_trace()
        # running_jct_dict = {}

        # first update running jcts of active jobs
        # for jid in job_state.active_jobs:
        # submit_time = job_state.active_jobs[jid]["submit_time"]
        # if "tracked_metrics" in job_state.active_jobs[jid]:
        # attained_service = job_state.active_jobs[jid]["tracked_metrics"][
        # "attained_service"
        # ]
        # else:
        # attained_service = 0

        # cluster_time_required = (
        # job_state.active_jobs[jid]["job_iteration_time"]
        # * job_state.active_jobs[jid]["job_total_iteration"]
        # )

        # running_jct_dict[jid] = (
        # simulator_time - submit_time - attained_service + cluster_time_required
        # )

        # simulator_time - submit_time = total_time_on_cluster
        # total_time_on_cluster - attained_service = total_time_spend_waiting
        # cluster_time_required = total_useful_time_needed
        # running_jct = total_useful_time_needed + total_time_spent_waiting

        # we will add pending jobs when we accept them in the cluster
        current_simulation_time = simulator_time
        admission_policy.simulator_time = current_simulation_time
        scheduling_policy.simulator_time = current_simulation_time
        completed_jobs = list()
        # for round_iteration in range(rounds_to_simulate)
        sum_cluster_time = 0
        sum_delayed_time = 0
        # while True:
        for _ in range(2):
            print("Current Name {}".format(current_name))
            # run simulation until no job remains
            accepted_jobs = admission_policy.accept(
                pending_jobs, cluster_state, job_state
            )
            job_state.add_new_jobs(accepted_jobs)
            # print("Add jobs")
            new_job_schedule = scheduling_policy.schedule(job_state, cluster_state)
            # print("New schedule")
            to_suspend, to_launch = placement_policy.place(
                job_state, cluster_state, new_job_schedule
            )
            # print("Place")

            # we still need to mark which jobs are now running and which are not
            exec_jobs(to_launch, to_suspend, cluster_state, job_state)
            # print("exec jobs")
            # now based on the above outputs we need to make our own simpler simulation logic
            for job_id in job_state.active_jobs:
                job_exit = False
                if job_state.active_jobs[job_id]["is_running"]:
                    # update metrics
                    total_iterations_in_round = (
                        round_duration
                        / job_state.active_jobs[job_id]["job_iteration_time"]
                    )
                    attained_service = (
                        job_state.active_jobs[job_id]["tracked_metrics"][
                            "attained_service"
                        ]
                        + round_duration
                    )

                    per_iteration_time = job_state.active_jobs[job_id][
                        "job_iteration_time"
                    ]

                    total_iteration_achieved = (
                        total_iterations_in_round
                        + job_state.active_jobs[job_id]["job_executed_iteration"]
                    )
                    if (
                        total_iteration_achieved
                        >= job_state.active_jobs[job_id]["job_total_iteration"]
                    ):
                        job_exit = True

                    job_state.active_jobs[job_id][
                        "job_executed_iteration"
                    ] = total_iteration_achieved

                    # now update the metrics
                    if job_exit == True:
                        job_state.active_jobs[job_id]["tracked_metrics"] = {
                            "attained_service": attained_service,
                            "job_exit": True,
                            "per_iter_time": per_iteration_time,
                        }

                    if not job_exit:
                        job_state.active_jobs[job_id]["tracked_metrics"] = {
                            "attained_service": attained_service,
                            "per_iter_time": per_iteration_time,
                        }
                    sum_cluster_time += (
                        job_state.active_jobs[job_id]["job_executed_iteration"]
                        / job_state.active_jobs[job_id]["job_total_iteration"]
                    )

                else:
                    if "queue_delay" not in job_state.active_jobs[job_id]:
                        # first time delayed
                        job_state.active_jobs[job_id]["queue_delay"] = (
                            current_simulation_time
                            - job_state.active_jobs[job_id]["submit_time"]
                        )
                    else:
                        job_state.active_jobs[job_id]["queue_delay"] += round_duration

                    sum_delayed_time += job_state.active_jobs[job_id]["queue_delay"] / (
                        job_state.active_jobs[job_id]["job_iteration_time"]
                        * job_state.active_jobs[job_id]["job_total_iteration"]
                    )

                    # print("Job State {}".format(job_state.active_jobs))
            # print("Job active")

            # for jobs in running_jct_dict:
            # if jobs in completed_jobs:
            # continue
            # elif job_state.active_jobs[jobs]["is_running"]:
            # continue

            # else:
            # running_jct_dict[jobs] += round_duration

            # find jobs to terminate
            jid_to_terminate = list()
            # for jid in job_state.active_jobs:
            # if "tracked_metrics" in job_state.active_jobs[jid]:
            # if (
            # job_state.active_jobs[jid]["tracked_metrics"].get("job_exit")
            # == True
            # ):
            # jid_to_terminate.append(jid)
            # completed_jobs.append(jid)
            # new_running_jct_dict["-".join(current_name)][0] += (
            # current_simulation_time
            # - job_state.active_jobs[jid]["submit_time"]
            # )
            # new_running_jct_dict["-".join(current_name)][1] += 1
            # print("Termination")
            for jid in jid_to_terminate:
                # free GPU
                _free_gpu_by_jobid(jid, cluster_state.gpu_df)
                job_state.active_jobs.pop(jid)

            # print("terminate")
            current_simulation_time += round_duration  # round duration
            print("Active Jobs {}".format(len(job_state.active_jobs)))

            if len(job_state.active_jobs) == 0:
                # exit if number of jobs is zero
                new_running_jct_dict["-".join(current_name)][0] = sum_cluster_time
                new_running_jct_dict["-".join(current_name)][1] = sum_delayed_time
                break
            else:
                new_running_jct_dict["-".join(current_name)][0] = sum_cluster_time
                new_running_jct_dict["-".join(current_name)][1] = sum_delayed_time
        # mean_running_jct = sum(running_jct_dict.values()) / len(
        # running_jct_dict.values()
        # )

        # combination_dict_avg_jct["-".join(current_name)] = mean_running_jct

    # import ipdb

    # ipdb.set_trace()
    # with open("prediction_jct_load_8.jsonl", "a") as fout:
    # dict_out = json.dumps({simulator_time: combination_dict_avg_jct})
    # fout.write(dict_out)
    # fout.write("\n")
    # import ipdb

    # ipdb.set_trace()
    return new_running_jct_dict

    # for jid in job_state.active_jobs:
    # active jobs really just has list of active jobs
    # if job_state.active_jobs


def exec_jobs(
    jobs_to_launch: dict,
    jobs_to_terminate: list,
    cluster_state,
    active_jobs,
) -> None:
    """
    First terminates the jobs. Then marks the jobs to launch.
    Args:
        jobs_to_launch: {Job_ID: [GPUs to launch]}
        jobs_to_terminate : List of Job IDs to Terminate
        cluster_state : ClusterState class
        active_jobs: JobState
    Return:
      None
    """
    terminate_list_id = list()
    terminate_ipaddr = list()
    terminate_simulation = list()
    for jid in jobs_to_terminate:
        # find ipaddresses for corresponding jobs to terminate
        running_ipddr = list(set(_find_ipaddr_by_job_ids(jid, cluster_state.gpu_df)))
        terminate_list_id.extend([jid] * len(running_ipddr))
        terminate_ipaddr.extend(running_ipddr)
        terminate_simulation.append(active_jobs.active_jobs[jid]["simulation"])
        # mark the job that is running is false
        active_jobs.active_jobs[jid]["is_running"] = False
        active_jobs.active_jobs[jid]["rank_0_ip"] = None
        # the job was suspended
        active_jobs.active_jobs[jid]["suspended"] = 1
        # mark corresponding GPUs on which the jobs are running as
        # available
        _free_gpu_by_jobid(jid, cluster_state.gpu_df)
    # don't need because simulation !
    # self.comm_node_manager.terminate_jobs(
    # terminate_list_id, terminate_ipaddr, terminate_simulation
    # )

    # jobs terminated

    for jid in jobs_to_launch:
        gpus_to_launch = jobs_to_launch[jid]
        ipaddress_to_launch = _find_ipaddr_by_gpu_ids(
            gpus_to_launch, cluster_state.gpu_df
        )
        local_gpu_ids = _find_local_gpu_id(gpus_to_launch, cluster_state.gpu_df)
        # no need because simulation !
        # self.comm_node_manager.launch_job(
        # jid, active_jobs.active_jobs[jid], local_gpu_ids, ipaddress_to_launch
        # )
        active_jobs.active_jobs[jid]["is_running"] = True
        active_jobs.active_jobs[jid]["rank_0_ip"] = ipaddress_to_launch[0]
        if "suspended" in active_jobs.active_jobs[jid]:
            active_jobs.active_jobs[jid]["suspended"] = 0

        _mark_gpu_in_use_by_gpu_id(gpus_to_launch, jid, cluster_state.gpu_df)


def _find_ipaddr_by_job_ids(job_id: str, gpu_df: pd.DataFrame) -> List[str]:
    """
    Given a jobID finds the ip-addresses on which the job runs.
    Args:
        job_id: ID of the job to find corresponding ipaddress
    Returns:
        List of IP addresses on which the job is running
    """
    return gpu_df[gpu_df["JOB_IDS"] == job_id]["IP_addr"].tolist()


def _find_ipaddr_by_gpu_ids(gpu_ids: List[int], gpu_df: pd.DataFrame) -> List[str]:
    """
    Return the IP address for given GPU IDs

    Args:
        gpu_ids: GPU ids to search
    Returns:
        List of IP addresses for corresponding gpu_ids
    """

    ipaddress_to_launch = list()
    for gid in gpu_ids:
        gid_ipaddr = gpu_df[gpu_df["GPU_ID"] == gid]["IP_addr"].tolist()
        assert len(gid_ipaddr) == 1, "Multiple IP addr for same GPU, something wrong"

        ipaddress_to_launch.extend(gid_ipaddr)
    return ipaddress_to_launch


def _free_gpu_by_jobid(job_id: int, gpu_df: pd.DataFrame) -> None:
    """
    Marks the corresponding GPU free for a given job ID
    Args:
        job_id: ID of the job to terminate
    """
    gpu_df.loc[gpu_df["JOB_IDS"] == job_id, ["JOB_IDS", "IN_USE"]] = (
        None,
        False,
    )
    return None


def _mark_gpu_in_use_by_gpu_id(
    gpu_id_list: List[int], job_id: int, gpu_df: pd.DataFrame
) -> None:
    """
    Marks the corresponding GPU in use for a given job ID
    Args:
        gpu_id_list : List of GPU ID's to terminate
        job_id: ID of the job to terminate
    """
    gpu_df.loc[gpu_df["GPU_ID"].isin(gpu_id_list), ["JOB_IDS", "IN_USE"]] = (
        job_id,
        True,
    )
    return None


def _find_local_gpu_id(global_gpu_ids: List[int], gpu_df: pd.DataFrame) -> List[int]:
    """
    Given a list of Global GPU ID's find the corresponding local GPU id's

    Args:
        global_gpu_ids: Global GPU ID's
    Returns:
        local_gpu_ids: Local GPU id's corresponding to that value
    """
    local_gpu_id = list()
    # TODO: Get rid of this for loop using .isin
    for gid in global_gpu_ids:
        lgid = gpu_df[gpu_df["GPU_ID"] == gid]["Local_GPU_ID"].tolist()

        assert (
            len(lgid) == 1
        ), "Multiple Local GPUs for same global GPU ID, something wrong"

        local_gpu_id.extend(lgid)

    return local_gpu_id
