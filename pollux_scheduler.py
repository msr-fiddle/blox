import os
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

from schedulers.pollux_lib.utils import update_allocation_info
def parse_args(parser):
    """
    parser : argparse.ArgumentParser
    return a parser with arguments
    """
    parser.add_argument(
        "--scheduler", default="Pollux", type=str, help="Name of the scheduling strategy"
    )

    parser.add_argument(
        "--node-manager-port", default=50052, type=int, help="Node Manager RPC port"
    )
    parser.add_argument(
        "--central-scheduler-port",
        default=50051,
        type=int,
        help="Central Scheduler RPC Port",
    )

    parser.add_argument(
        "--simulator-rpc-port",
        default=50050,
        type=int,
        help="Simulator RPC port to fetch ",
    )

    parser.add_argument(
        "--scheduler-name",
        default="Pollux",
        type=str,
        help="Name of the scheduling strategy",
    )

    parser.add_argument(
        "--placement-name",
        default="Pollux",
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
        "--round-duration", type=int, default=60, help="Round duration in seconds" # may be same as "interval" in Pollux
    )
    parser.add_argument(
        "--start-id-track", type=int, default=0, help="Starting ID to track"
    )
    parser.add_argument(
        "--stop-id-track", type=int, default=4000, help="Stop ID to track"
    )
    parser.add_argument(
        "--interference", type=float, default=0.0, help="job slowdown due to interference, as used on Pollux"
    )


    args = parser.parse_args()
    return args


def main(args):
    # The scheduler runs Goodput optimizing scheduling and placement policy and accept all admission policy
    # scheduling and placement policies are combined, both implemented in schedulers.Pollux()

    # Running Pollux scheduling policy
    # placement_policy = placement.JobPlacement(args)  # XY: seems nothing done at init
    scheduling_policy = schedulers.Pollux(args)
    admission_policy = admission_control.acceptAll(args)
    if args.simulate:
        # for simulation, we get the config from the simulator
        # The config helps in providing file names and initialize
        blox_instance = BloxManager(args)
        # XY: calls simulator server to get sim_config, i.e., simulator_simple.GetConfig
        new_config = blox_instance.rmserver.get_new_sim_config()
        print(f"New config {new_config}")
        if args.scheduler_name == "":
            # terminate the blox instance before exiting
            # if no scheduler provided break
            blox_instance.terminate_server()
            print("No Config Sent")
            sys.exit()

        blox_instance.scheduler_name = new_config["scheduler"]
        blox_instance.load = new_config["load"] # XY: "Number of jobs per hour", args.jobs_per_hour in simulator_simple.py

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
        print(f"scheduler_name is {job_state.scheduler_name}")
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
            blox_instance.update_metrics(cluster_state, job_state)
            # XY: call blox manager, which will call simulator server to GetJobs, will get a dict of job_to_run
            new_jobs = blox_instance.pop_wait_queue(args.simulate)
            # get simulator jobs
            accepted_jobs = admission_policy.accept(new_jobs, cluster_state, job_state)
            job_state.add_new_jobs(accepted_jobs)
            # prune jobs - get rid of finished jobs
            utils.prune_jobs(job_state, cluster_state, blox_instance) # XY message to author: some operations in this call overlaps with those in "blox_instance.update_metrics"

            ### XY: call Pollux scheduler_placement
            # Input: job_state, cluster_state
            # Output: a dict containing to_suspend, to_launch, allocations

            schedule_info = scheduling_policy.schedule(job_state, cluster_state)

            to_suspend = schedule_info["to_suspend"]
            to_launch = schedule_info["to_launch"]

            ### XY: end call Pollux scheduler_placement

            utils.collect_custom_metrics(
                job_state, cluster_state, {"num_preemptions": len(to_suspend)}
            )

            utils.collect_cluster_job_metrics(job_state, cluster_state)
            # check if we have finished every job to track
            utils.track_finished_jobs(job_state, cluster_state, blox_instance) # XY message to author: some operations in this call overlaps with those in "blox_instance.update_metrics"
            # execute jobs
            blox_instance.exec_jobs(to_launch, to_suspend, cluster_state, job_state)

            # XY: Update job and cluster metrics according to the new allocation
            update_allocation_info(schedule_info["allocations"], job_state, cluster_state)

            # update time
            simulator_time += args.round_duration
            job_state.time += args.round_duration
            cluster_state.time += args.round_duration
            blox_instance.time += args.round_duration

if __name__ == "__main__":
    args = parse_args(
        argparse.ArgumentParser(description="Arguments for Starting the scheduler")
    )

    main(args)
