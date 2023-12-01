import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
import os
import argparse
import schedulers
from placement import placement
import admission_control
from blox import BloxManager
from blox import ClusterState
from blox import JobState
import blox.utils as utils


def main(args):
    admission_policy = admission_control.acceptAll(args)
    placement_policy = placement.JobPlacement(args)
    scheduling_policy = schedulers.Las(args)

    # blox_mgr = BloxManager(args)

    if args.simulate:
        while True:
            # need this instatiated because it holds connection to grpc
            blox_mgr = BloxManager(args)
            new_config = blox_mgr.rmserver.get_new_sim_config()
            if args.scheduler_name == "":
                # when the blox manager sends no name we terminate
                blox_mgr.terminate_server()
                break
            if new_config["scheduler"] == "":
                break
            args.scheduler_name = new_config["scheduler"]
            args.load = new_config["load"]
            args.placement_name = new_config["placement_policy"]
            args.acceptance_policy = new_config["acceptance_policy"]
            args.start_id_track = new_config["start_id_track"]
            args.stop_id_track = new_config["stop_id_track"]

            # updating newly recieved config
            blox_mgr.reset(args)
            cluster_state = ClusterState(args)
            job_state = JobState(args)

            # choosing which policy to run
            if args.placement_name == "Place":
                placement_policy = placement.JobPlacement(args)
            else:
                raise NotImplemented(
                    f"Placement Policy {args.placement_policy} not Implemented"
                )

            if args.acceptance_policy == "AcceptAll":
                admission_policy = admission_control.acceptAll(args)

            elif args.acceptance_policy == "LoadBasedAccept-1.0x":
                admission_policy = admission_control.loadBasedAccept(args, 1.0)
            elif args.acceptance_policy == "LoadBasedAccept-1.2x":
                admission_policy = admission_control.loadBasedAccept(args, 1.2)
            elif args.acceptance_policy == "LoadBasedAccept-1.4x":
                admission_policy = admission_control.loadBasedAccept(args, 1.4)
            elif args.acceptance_policy == "LoadBasedAccept-1.5x":
                admission_policy = admission_control.loadBasedAccept(args, 1.5)
            else:
                raise NotImplemented(f"{args.acceptance_policy} not Implemented")

            if args.scheduler_name == "Las":
                scheduling_policy = schedulers.Las(args)

            elif args.scheduler_name == "Fifo":
                scheduling_policy = schedulers.Fifo(args)

            elif args.scheduler_name == "Srtf":
                scheduling_policy = schedulers.Srtf(args)

            elif args.scheduler_name == "Optimus":
                scheduling_policy = schedulers.Optimus(args)
            elif args.scheduler_name == "Tiresias":
                scheduling_policy = schedulers.Tiresias(args)

            else:
                raise NotImplemented(f"{args.scheduler_name} not Implemented")

            simulator_time = 0

            os.environ["sched_policy"] = args.scheduler_name
            os.environ["sched_load"] = str(args.load)

            while True:
                if blox_mgr.terminate:
                    # print JCT and Avg
                    blox_mgr.terminate_server()
                    print("Terminate current config {}".format(args))
                    break

                blox_mgr.update_cluster(cluster_state)
                # TODO: Clean update metrics

                blox_mgr.update_metrics(cluster_state, job_state)
                new_jobs = blox_mgr.pop_wait_queue(args.simulate)

                accepted_jobs = admission_policy.accept(
                    new_jobs,
                    cluster_state,
                    job_state,
                )

                job_state.add_new_jobs(accepted_jobs)
                new_job_schedule = scheduling_policy.schedule(job_state, cluster_state)
                to_suspend, to_launch = placement_policy.place(
                    job_state, cluster_state, new_job_schedule
                )

                # collecting custom metrics
                utils.collect_custom_metrics(
                    job_state, cluster_state, {"num_preemptions": len(to_suspend)}
                )

                blox_mgr.exec_jobs(to_launch, to_suspend, cluster_state, job_state)
                if args.simulate:
                    simulator_time += args.round_duration
                    blox_mgr.simulator_time += args.round_duration
                    job_state.simulator_time += args.round_duration
                    admission_policy.simulator_time += args.round_duration


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
        "--acceptance-policy",
        default="accept_all",
        type=str,
        help="Name of acceptance policy",
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


def _get_avg_jct(time_dict):
    """
    Fetch the avg jct from the dict
    """
    values = list(time_dict.values())
    count = 0
    jct_time = 0
    for v in values:
        jct_time += v[1] - v[0]
        count += 1

    return jct_time / count


if __name__ == "__main__":
    args = parse_args(
        argparse.ArgumentParser(description="Arguments for Starting the scheduler")
    )

    main(args)
