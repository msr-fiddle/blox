import time
import subprocess

while True:
    with open("job_file_db.txt", "r") as fopen:
        data = fopen.read()
        print(data)
        proc = subprocess.Popen(
            f"{command_to_run}  {' '.join(str(i) for i in launch_params)}  2>&1 | tee /dev/shm/job_{job_id}_local_gpu_{local_gpu_id}.log",
            stdout=subprocess.PIPE,
            # stderr=subprocess.STDOUT,
            start_new_session=True,
            shell=True,
        )
    time.sleep(10)
