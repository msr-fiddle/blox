# Blox

Blox provides a modular framework for implementing research schedulers. Blox includes all infrastructure which is needed to implement a scheduler for deep learning jobs. More than just including infrastructure blox provides modular abstractions which can easily swapped/modified to enable researchers to implement novel scheduling policies.

### Abstraction Provided
* Job Admission Policy - Allows researchers to implement any admission policy and provides an interface to accept jobs.
* Cluster Management - Handle addition or deletion of available nodes.
* Job Scheduling Policy - Implement scheduling policy, i.e., deciding which jobs to run.
* Job Placement Policy - Implement Placement policy, i.e., deciding which specific GPUs to run a job.
* Job Launch and Preemption - Launch/Preempt specific jobs on any node in the cluster.
* Metric Collection - Collect any metric that are needed to make informed placement or launch decisions.

### Using blox

Components of blox are designed to be easily swappable based on different objective. However, from experience, most of existing deep learning schedulers can be implemented by adding a new scheduling policy and modifying the placement.



### Blox Simulator 

For large scale experiments it is often the practise to simulate how a policy will behave under different loads. Blox also provides a built in simulator for this case. 
Simulator for blox is implemented in _simulator.py_. Researchers can configure the load values, load arrival patterns.


### Blox utilities

Blox already has several plotting and metric parsing utilities. Based on configurations, blox will automatically output metrics like Job Completion Time and Job Completion CDFs. 


### Running Blox

Blox has two modes for running. One real cluster workload and second simulator. 

##### Cluster mode

1. For running in cluster mode. First on one of the nodes in the cluster launch the resource manager with following command. 
```
python resource_manager.py --scheduler fifo --plot --exp-prefix test --round-duration 300
```
2. On each of the nodes launch the node manager.
```
python node_manager.py --ipaddr resource_managers_ip
```
3. Post this you can use the *deployment/job_submit_scripts.py* to launch jobs on each node

##### Simulation mode
1. For running in the simulation model. We will launch the resource manager in the simulator mode. 
```
python resource_manager.py --scheduler fifo --plot --exp-prefix test --round-duration 300 --simulate
```
2. Post this launch the simulator to send load to simulate. 
```
python simulator.py --cluster-job-log microsof_job_log_file --sim-type trace-synthetic --jobs-per-hour integer_value --exp-prefix test
```

### Details for reproducing results for artifacts
These are instructions for reproducing artifacts for Blox.
#### Installation 
Blox uses gRpc, Matplotlib to communicate and Plot several collected Metric. 
We suggest the users to create a virtual environment to install the dependencies.
```
pip install grpcio
pip install matplotlib
pip install pandas==1.4.0
pip install grpcio-tools
```

###### Running Blox Code
To perform simulation.
In one terminal window.
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```
In second terminal window. 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python blox_new_flow_multi_run.py --simulate --load 6 --exp-prefix test
```
The above experiment will take around 8hrs to run and will generate CDF, JCT and runtime for Fifo, LAS and Optimus Scheduler as in Figure 6. 


For running LAS scheduler with different acceptance policy. This will provide Avg JCTs for Figure 12 and Figure 13.
In one terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_dual_load.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```
In second terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python blox_new_flow_multi_run.py --simulate --load 6 --exp-prefix test
```


### License

Copyright (c) Microsoft Corporation. All rights reserved.

Licensed under the [MIT](LICENSE.txt) license.
