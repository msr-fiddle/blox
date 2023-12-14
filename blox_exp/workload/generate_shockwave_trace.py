import os
import sys

import argparse
import csv
import math
import numpy as np
import random
from random import choice


def generate_interarrival_time(rng, lam):
    return -math.log(1.0 - rng.random()) * lam


def _generate_scale_factor(rng):
    # Sample the scale factor from the Shockwave repo.
    scale_factor = 1
    r = rng.uniform(0, 1)
    if 0.6 < r <= 0.9:
        scale_factor = 2
    elif 0.9 < r <= 0.99:
        scale_factor = 4
    elif 0.99 < r:
        scale_factor = 8
    return scale_factor


def _generate_duration(durations, rng):
    # Sample the job duration from the Shockwave repo.
    duration_prob = [0.72, 0.2, 0.05, 0.03]
    duration_boundary = [0.2, 0.5, 0.9, 1.0]

    num_durations = len(durations)
    num_small_durations = round(num_durations * duration_boundary[0])
    num_medium_durations = round(num_durations * duration_boundary[1])
    num_large_durations = round(num_durations * duration_boundary[2])

    r = rng.uniform(0, 1)

    if r < duration_prob[0]:
        durations = durations[:num_small_durations]
    elif duration_prob[0] <= r < sum(duration_prob[:2]):
        durations = durations[num_small_durations:num_medium_durations]
    elif sum(duration_prob[:2]) <= r < sum(duration_prob[:3]):
        durations = durations[num_medium_durations:num_large_durations]
    else:
        durations = durations[num_large_durations:]

    choice = np.random.choice(durations)

    return round(3600 * choice)


def main(args):
    output_file = f"poisson_trace_{args.num_jobs}+{args.lam}.csv"

    np.random.seed(args.seed)
    job_generator = random.Random()
    job_generator.seed(args.seed)

    interarrival_time_generator = random.Random()
    interarrival_time_generator.seed(args.seed + 1)

    duration_generator = random.Random()
    duration_generator.seed(args.seed + 2)

    scale_factor_generator = random.Random()
    scale_factor_generator.seed(args.seed + 3)

    single_gpu_model_list = ['resnet50', 'vgg19', 'DCGAN', 'PointNet']
    multi_gpu_model_list = ['Bert-Large', 'GPT2-Medium']
    all_model_list = single_gpu_model_list + multi_gpu_model_list
    batch_size_range = [32, 64, 128]

    fields = ['job_id', 'num_gpus', 'submit_time', 'duration', 'model', 'batch_size']
    rows = []
    init_time = random.randint(10, 100)  # randomly set the init time
    prev_arrival_time = None
    for i in range(args.num_jobs):
        scale_factor = _generate_scale_factor(scale_factor_generator)
        if scale_factor == 1:
            model_name = choice(single_gpu_model_list)
        elif 1 < scale_factor <= 4:
            model_name = choice(all_model_list)
        elif scale_factor > 4:
            model_name = choice(multi_gpu_model_list)
        else:
            raise ValueError("scale factor is not considered now.")
        batch_size = choice(batch_size_range)
        durations = np.linspace(
            args.min_duration, args.max_duration, args.num_durations
        )
        run_time = int(_generate_duration(durations, duration_generator))
        if prev_arrival_time is None:
            arrival_time = 0
        elif args.lam > 0:
            interarrival_time = generate_interarrival_time(interarrival_time_generator, args.lam)
            arrival_time = prev_arrival_time + int(interarrival_time * 60)
        else:
            raise ValueError("lam is not equal to zero")
        prev_arrival_time = arrival_time
        start_time = init_time + arrival_time
        # ('job_id', 'num_gpus', 'submit_time', 'duration', 'model', 'batch_size')
        row = [i + 1, scale_factor, start_time, run_time, model_name, batch_size]
        rows.append(row)

    with open(output_file, 'w') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the fields
        csvwriter.writerow(fields)
        # writing the data rows
        csvwriter.writerows(rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate synthetic trace')
    parser.add_argument('--num_jobs', type=int, default=900,
                        help='Number of jobs to generate')
    parser.add_argument('-l', '--lam', type=float, default=20.0,
                        help='Lambda for Poisson arrival rate. '
                             'lam=3 means submitting a job every 3 minutes')
    parser.add_argument('--seed', type=int, default=0,
                        help='Random seed')
    parser.add_argument('-a', '--min_duration', type=float, default=0.2,
                        help='Minimum job duration in hours')
    parser.add_argument('-b', '--max_duration', type=float, default=5,
                        help='Maximum job duration in hours')
    parser.add_argument('-n', '--num_durations', type=int, default=100,
                        help='Number of possible job durations')

    args = parser.parse_args()
    num_jobs_list = [10]
    lam_list = [0.5]
    for num_jobs in num_jobs_list:
        for lam in lam_list:
            args.num_jobs = num_jobs
            args.lam = lam
            main(args)
