# Reproduce experimental results for Pollux on Blox

### Run Pollux on Blox

Once the installation steps from [INSTALL.md](https://github.com/msr-fiddle/blox/blob/main/INSTALL.md) are completed, open two separate terminals, and ensure that your virtual environment is active in both terminals.

In the first terminal, run the simulator using the following command:

```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_simple.py --cluster-job-log workload_pollux/workload-6.csv --sim-type trace-synthetic --jobs-per-hour 1 --exp-prefix 30s > out_sim.txt
```

In the second terminal, run the Pollux scheduler using the following command:

```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python pollux_scheduler.py --simulate --load 1 --exp-prefix 30s --round-duration 30 > out_sch.txt
```

Once the execution completes, you should see new data files with ```30s``` as the prefix. In the same manner, use ```1min```, ```2min```, ```4min```, and ```8min``` as the ```exp-prefix``` for ```60```, ```120```, ```240```, and ```480``` as the ```round-length``` respectively. This would complete the creation of the data for simulating Pollux using Blox.

### Compare Pollux on Blox with Pollux Authors' Implementation

The original Pollux data was sourced from the [adaptdl](https://github.com/petuum/adaptdl/tree/osdi21-artifact/simulator/results-period) repository.

All data files must be located in the same directory as ```plot_pollux.py```. Then, run ```python plot_pollux.py``` to view the plot that compares the results of running Pollux on Blox with the results presented by the authors of Pollux.