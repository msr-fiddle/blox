import time
import subprocess

while True:
    with open("job_file_db.txt", "r") as fopen:
        data = fopen.read()
        print(data)
    time.sleep(10)
