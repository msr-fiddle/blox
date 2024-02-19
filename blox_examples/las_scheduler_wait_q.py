import copy
import argparse

# from job_admission_policy import accept_all
from scheduling_policy import las
from placement_policy import placement
from acceptance_policy import load_based_accept
from blox import ClusterState, JobState, BloxResourceManager
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
    # these are policies initialized
    placement_policy = placement.JobPlacement()
    scheduler_policy = las.Las(args)
    acceptance_policy = load_based_accept.loadBasedAccept(args)

    # these are blox state initialization
    # metrics_collector = metrics_collector(args)
    if args.simulate:
        while True:
            blox_instance = BloxResourceManager(args)
            new_config = blox_instance.rmserver.get_new_sim_config()
            if args.scheduler_name == "":
                # terminate the blox instance before exiting
                blox_instance.terminate_server()
                break

            blox_instance.scheduler_name = new_config["scheduler"]
            blox_instance.load = new_config["load"]

            args.scheduler_name = new_config["scheduler"]
            args.load = new_config["load"]
            args.start_id_track = new_config["start_id_track"]
            args.stop_id_track = new_config["stop_id_track"]
            # terminate the blox instance grpc server before reinitialize
            # blox_instance.terminate_server()
            # blox_instance = BloxResourceManager(args)
            cluster_state = ClusterState(blox_instance, args)
            job_state = JobState(blox_instance, args)

            simulator_time = 0
            while True:
                # get new nodes for the cluster
                if blox_instance.terminate:
                    blox_instance.terminate_server()
                    print("Terminate current config {}".format(args))
                    break
                new_nodes = cluster_state.get_new_nodes()
                cluster_state.update(new_nodes)
                print("Cluster State Added")
                # get simulator jobs
                if len(cluster_state.gpu_df) > 0:
                    # only get jobs when we have the scheduler
                    new_jobs = job_state.get_new_jobs_sim()
                    # print("Got new jobs sim")
                    accepted_jobs = acceptance_policy.admit(
                        new_jobs, cluster_state, job_state, simulator_time
                    )
                    # print("Accepted jobs {}".format(accepted_jobs))
                    job_state.add(accepted_jobs)
                    # job_state.add(new_jobs)
                # TODO: Add acceptance policy
                # add new jobs
                # get latest metrics

                latest_metrics = utils.get_metrics(
                    blox_instance, cluster_state, job_state
                )
                # update metrics
                # print("Simulator time = {}".format(simulator_time))
                # import ipdb
                # ipdb.set_trace()
                # print("Latest Metrics {}".format(latest_metrics))
                job_state.update_metrics(latest_metrics)
                # prune jobs - get rid of finished jobs
                utils.prune_jobs(job_state, cluster_state, blox_instance)
                # perform scheduling
                new_job_schedule = scheduler_policy.schedule(
                    copy.deepcopy(job_state.active_jobs),
                    copy.deepcopy(cluster_state.server_map),
                    copy.deepcopy(cluster_state.gpu_df),
                )
                # get placement
                jobs_to_suspend, jobs_to_launch = placement_policy.place_jobs(
                    copy.deepcopy(job_state.active_jobs),
                    new_job_schedule,
                    copy.deepcopy(cluster_state.server_map),
                    copy.deepcopy(cluster_state.gpu_df),
                )

                # collect job level metrics
                utils.collect_custom_metrics(
                    job_state,
                    cluster_state,
                    {
                        "num_preemptions": len(jobs_to_suspend),
                        "length_wait_q": len(acceptance_policy.overflow_queue),
                    },
                )
                utils.collect_cluster_job_metrics(job_state, cluster_state)
                # check if we have finished every job to track
                utils.track_finished_jobs(job_state, cluster_state, blox_instance)
                # execute jobs
                utils.execute_jobs(
                    jobs_to_launch,
                    jobs_to_suspend,
                    cluster_state,
                    job_state,
                    blox_instance,
                )
                # update time
                simulator_time += args.round_duration
                job_state.simulator_time += args.round_duration
                cluster_state.simulator_time += args.round_duration
                blox_instance.simulator_time += args.round_duration


if __name__ == "__main__":
    args = parse_args(
        argparse.ArgumentParser(description="Arguments for Starting the scheduler")
    )

    main(args)
