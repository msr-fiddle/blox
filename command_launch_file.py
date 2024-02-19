import time
import subprocess

while True:
    with open("job_file_db.txt", "r") as fopen:
        data = fopen.read()
        print(data)
        proc = subprocess.Popen(
            data,
            stdout=subprocess.PIPE,
            # stderr=subprocess.STDOUT,
            start_new_session=True,
            shell=True,
        )
    time.sleep(10000000)
