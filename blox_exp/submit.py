import pandas as pd
import time

from job import Job


def preprocess_job(new_job):
    new_job["exclusive"] = 1
    new_job["submit_time"] = new_job["job_arrival_time"]
    new_job["num_GPUs"] = new_job["job_gpu_demand"]
    return new_job


def main():
    df = pd.read_csv(
        filepath_or_buffer="/global/homes/s/songbian/Megatron-Resource/blox_exp/workload/poisson_trace_10+0.5.csv",
        dtype={
            "job_id": int,
            "num_gpus": int,
            "submit_time": int,
            "duration": int,
            "model": str,
            "batch_size": str,
        },
    )

    jobs = []
    for _, row in df.iterrows():
        job = Job(
            job_id=row.job_id,
            job_arrival_time=row.submit_time,
            job_gpu_demand=row.num_gpus,
            job_iteration_time=1,
            job_total_iteration=row.duration,
            job_model=row.model,
            batch_size=row.batch_size,
        )
        jobs.append(job)

    current_time = 0
    jcounter = 0
    current_job = None
    while True:
        if current_job is None:
            new_job = preprocess_job(jobs[jcounter].__dict__)
            current_job = new_job
            print(current_job)
        if current_job["submit_time"] <= current_time:
            # submit_job
            import ipdb

            ipdb.set_trace()
            jcounter += 1
            if jcounter == len(jobs):
                break
            current_job = None
        else:
            time.sleep(1)
            current_time += 1


if __name__ == "__main__":
    main()
