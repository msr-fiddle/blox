TODO: Update this file with directions on how to reproduce results

conda activate pollux
rm -f out_sim.txt
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_simple.py --cluster-job-log workload_pollux/workload-6.csv --sim-type trace-synthetic --jobs-per-hour 1 --exp-prefix test > out_sim.txt

conda activate pollux
rm -f out_sch.txt
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python pollux_scheduler.py --simulate --load 1 --exp-prefix test > out_sch.txt