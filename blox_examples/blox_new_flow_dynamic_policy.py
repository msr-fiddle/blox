import os
import json
import copy
import argparse
import schedulers
from placement import placement
import admission_control
from blox import BloxManager
from blox import ClusterState
from blox import JobState
import blox.utils as utils
from predict_simulation import get_metric_to_collect


def main(args):
    admission_policy = admission_control.acceptAll(args)
    placement_policy = placement.JobPlacement(args)
    scheduling_policy = schedulers.Las(args)

    scheduler_choices = [
        schedulers.Las(args),
        schedulers.Fifo(args),
        schedulers.Srtf(args),
    ]

    scheduler_choices_name = ["Las", "Fifo", "Srtf"]

    admission_choices = [
        admission_control.acceptAll(args),
        admission_control.loadBasedAccept(args, 1.2),
        admission_control.loadBasedAccept(args, 1.4),
    ]

    admission_choices_name = ["Accept All", "Load Based, 1.2x", "Load Based, 1.4x"]

    placement_choices = [placement.JobPlacement(args)]

    placement_choices_name = ["Place"]

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

            args.scheduler_name = new_config["scheduler"]
            args.load = new_config["load"]
            args.placement_name = new_config["placement_policy"]
            args.acceptance_policy = new_config["acceptance_policy"]
            args.start_id_track = new_config["start_id_track"]
            args.stop_id_track = new_config["stop_id_track"]

            running_jct_dict = dict()
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

            elif args.acceptance_policy == "LoadBasedAccept":
                admission_policy = admission_control.loadBasedAccept(args)
            else:
                raise NotImplemented(f"{args.acceptance_policy} not Implemented")

            if args.scheduler_name == "Las":
                scheduling_policy = schedulers.Las(args)

            elif args.scheduler_name == "Fifo":
                scheduling_policy = schedulers.Fifo(args)
            elif args.scheduler_name == "Srtf":
                scheduling_policy = schedulers.Srtf(args)
            else:
                raise NotImplemented(f"{args.scheduler_name} not Implemented")

            simulator_time = 0
            round_number = 0
            while True:
                if blox_mgr.terminate:
                    blox_mgr.terminate_server()
                    print("Terminate current config {}".format(args))
                    break

                blox_mgr.update_cluster(cluster_state)
                # TODO: Clean update metrics

                blox_mgr.update_metrics(cluster_state, job_state)
                new_jobs = blox_mgr.pop_wait_queue(args.simulate)

                # for prediction send jobs with acceptance policy
                # this will handle if we are performing predictions
                if round_number % 1 == 0:
                    if isinstance(admission_policy, admission_control.loadBasedAccept):
                        # extract jobs out of admission policy as well
                        copied_from_admission_policy = copy.deepcopy(
                            admission_policy.overflow_queue
                        )
                        copied_from_admission_policy.extend(new_jobs)
                        # import ipdb

                        # ipdb.set_trace()
                        new_jobs = copied_from_admission_policy

                    running_jct_dict = get_metric_to_collect(
                        new_jobs,
                        scheduler_choices,
                        admission_choices,
                        placement_choices,
                        job_state,
                        cluster_state,
                        1,
                        simulator_time,
                        scheduler_choices_name,
                        admission_choices_name,
                        placement_choices_name,
                        running_jct_dict,
                    )

                    # take average of each setup
                    get_prediction = dict()
                    for key in running_jct_dict:
                        total_jct, num_jobs = running_jct_dict[key]
                        if num_jobs == 0:
                            get_prediction[key] = 0
                        else:
                            get_prediction[key] = total_jct / num_jobs

                    min_timing = min(get_prediction, key=get_prediction.get)
                    print("Min Timing {}".format(min_timing))
                    with open(
                        "./fixed_dynamic_run_to_completion_sim.jsonl", "a"
                    ) as out_val:
                        write_dump = json.dumps({simulator_time: get_prediction})
                        out_val.write(write_dump)
                        out_val.write("\n")
                    min_timing_list = min_timing.split("-")

                    min_scheduler = min_timing_list[0]
                    min_admission_policy = min_timing_list[1]
                    min_placement_policy = min_timing_list[2]
                    # import ipdb

                    # ipdb.set_trace()
                    if min_placement_policy == "Place":
                        placement_policy = placement.JobPlacement(args)
                    else:
                        raise NotImplemented(
                            f"Placement Policy {args.placement_policy} not Implemented"
                        )

                    if min_admission_policy == "Accept All":
                        admission_policy = admission_control.acceptAll(args)

                    elif min_admission_policy == "Load Based, 1.4x":
                        admission_policy = admission_control.loadBasedAccept(args, 1.4)
                    elif min_admission_policy == "Load Based, 1.2x":
                        admission_policy = admission_control.loadBasedAccept(args, 1.2)
                    else:
                        raise NotImplemented(
                            f"{args.acceptance_policy} not Implemented"
                        )

                    if min_scheduler == "Las":
                        scheduling_policy = schedulers.Las(args)
                    elif min_scheduler == "Fifo":
                        scheduling_policy = schedulers.Fifo(args)
                    elif min_scheduler == "Srtf":
                        scheduling_policy = schedulers.Srtf(args)
                    else:
                        raise NotImplemented(f"{args.scheduler_name} not Implemented")

                    admission_policy.simulator_time = simulator_time
                    scheduling_policy.simulator_time = simulator_time

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
                round_number += 1


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


if __name__ == "__main__":
    args = parse_args(
        argparse.ArgumentParser(description="Arguments for Starting the scheduler")
    )

    main(args)
