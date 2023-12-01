import os
import time
import warnings
import sys
import copy
import argparse

warnings.simplefilter(action="ignore", category=FutureWarning)
# from job_admission_policy import accept_all
import schedulers
from placement import placement
import admission_control

# from acceptance_policy import load_based_accept
from blox import ClusterState, JobState, BloxManager
import blox.utils as utils


def parse_args(parser):
    """
    parser : argparse.ArgumentParser
    return a parser with arguments
    """
    parser.add_argument(
        "--scheduler", default="Las", type=str, help="Name of the scheduling strategy"
    )
    parser.add_argument(
        "--scheduler-name",
        default="Las",
        type=str,
        help="Name of the scheduling strategy",
    )

    parser.add_argument(
        "--placement-name",
        default="Las",
        type=str,
        help="Name of the scheduling strategy",
    )

    parser.add_argument(
        "--acceptance-policy",
        default="accept_all",
        type=str,
        help="Name of acceptance policy",
    )

    parser.add_argument(
        "--plot", action="store_true", default=False, help="Plot metrics"
    )
    parser.add_argument(
        "--exp-prefix", type=str, help="Unique name for prefix over log files"
    )

    parser.add_argument("--load", type=int, help="Number of jobs per hour")

    parser.add_argument("--simulate", action="store_true", help="Enable Simulation")

    parser.add_argument(
        "--round-duration", type=int, default=300, help="Round duration in seconds"
    )
    parser.add_argument(
        "--start-id-track", type=int, default=3000, help="Starting ID to track"
    )
    parser.add_argument(
        "--stop-id-track", type=int, default=4000, help="Stop ID to track"
    )

    args = parser.parse_args()
    return args


def main(args):
    # The scheduler runs consolidated placement, with las scheduler and accept all placement policy
    placement_policy = placement.JobPlacement(args)
    # Running LAS scheduling policy
    scheduling_policy = schedulers.Las(args)
    admission_policy = admission_control.acceptAll(args)
    # if args.simulate:
    # for simulation we get the config from the simulator
    # The config helps in providing file names and intialize
    blox_instance = BloxManager(args)
    if args.simulate:
        new_config = blox_instance.rmserver.get_new_sim_config()
        print(f"New config {new_config}")
        if args.scheduler_name == "":
            # terminate the blox instance before exiting
            # if no scheduler provided break
            blox_instance.terminate_server()
            print("No Config Sent")
            sys.exit()

        blox_instance.scheduler_name = new_config["scheduler"]
        blox_instance.load = new_config["load"]

        args.scheduler_name = new_config["scheduler"]
        args.load = new_config["load"]
        args.start_id_track = new_config["start_id_track"]
        args.stop_id_track = new_config["stop_id_track"]
        print(
            f"Running Scheduler {args.scheduler_name}\nLoad {args.load} \n Placement Policy {args.placement_name} \nAcceptance Policy {args.acceptance_policy} \nTracking jobs from {args.start_id_track} to {args.stop_id_track}"
        )
        blox_instance.reset(args)
    cluster_state = ClusterState(args)
    job_state = JobState(args)
    os.environ["sched_policy"] = args.scheduler_name
    os.environ["sched_load"] = str(args.load)
    simulator_time = 0
    while True:
        # get new nodes for the cluster
        if blox_instance.terminate:
            blox_instance.terminate_server()
            print("Terminate current config {}".format(args))
            break
        blox_instance.update_cluster(cluster_state)
        print("State of cluster {}".format(cluster_state.gpu_df))
        blox_instance.update_metrics(cluster_state, job_state)
        new_jobs = blox_instance.pop_wait_queue(args.simulate)
        # get simulator jobs
        accepted_jobs = admission_policy.accept(new_jobs, cluster_state, job_state)
        job_state.add_new_jobs(accepted_jobs)
        new_job_schedule = scheduling_policy.schedule(job_state, cluster_state)
        # prune jobs - get rid of finished jobs
        utils.prune_jobs(job_state, cluster_state, blox_instance)
        # perform scheduling
        new_job_schedule = scheduling_policy.schedule(job_state, cluster_state)
        # get placement
        to_suspend, to_launch = placement_policy.place(
            job_state, cluster_state, new_job_schedule
        )

        utils.collect_custom_metrics(
            job_state, cluster_state, {"num_preemptions": len(to_suspend)}
        )
        # collect cluster level metrics
        utils.collect_cluster_job_metrics(job_state, cluster_state)
        # check if we have finished every job to track
        utils.track_finished_jobs(job_state, cluster_state, blox_instance)
        # execute jobs
        blox_instance.exec_jobs(to_launch, to_suspend, cluster_state, job_state)
        # update time

        simulator_time += args.round_duration
        # if args.simulate:
        job_state.time += args.round_duration
        cluster_state.time += args.round_duration
        blox_instance.time += args.round_duration
        if not args.simulate:
            print("Time sleep")
            time.sleep(args.round_duration)


if __name__ == "__main__":
    args = parse_args(
        argparse.ArgumentParser(description="Arguments for Starting the scheduler")
    )

    main(args)
