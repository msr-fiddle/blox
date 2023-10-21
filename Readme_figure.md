### Blox
This is a step by step guide for reproducing results for each figure in Blox

#### Figure 6, Figure 7
In Figure 6 and Figure 7 we evaluate FiFo, Tiresias and Optimus on the open source Philly trace. 

In one terminal window.
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```
In second terminal window. 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python blox_new_flow_multi_run.py --simulate --load 6 --exp-prefix test
```

Once this simulation is over it will generate the Responsiveness and JCT for using FIFO, Tiresias and LAS scheduler.

#### Figure 12 and Figure 13


For running LAS scheduler with different acceptance policy. This will provide Avg JCTs for Figure 12 and Figure 13.
In one terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_dual_load.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```
In second terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python blox_new_flow_multi_run.py --simulate --load 6 --exp-prefix test
```


#### Figure 14 and 15 a) and b)

Running the following code will generate the code for runtime statistics for dynamic scheduler. 

For Figure 14 and Figure 15 b) - 
In one terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_dual_load.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```

In second terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python blox_new_flow_dynamic_policy.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```

Figure 15 a) - 

In one terminal  
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_dual_load_small_large.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```

In terminal  
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python blox_new_flow_dynamic_policy.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```


