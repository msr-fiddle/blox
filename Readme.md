# Blox
This repository contains the source code implementation of the Eurosys 2024 paper "Blox: A Modular Toolkit for Deep Learning Schedulers". This work was done as part of Microsoft Research's [Project Fiddle](https://https://aka.ms/msr-fiddle). This source code is available under the [MIT License](LICENSE.txt).

Blox provides a modular framework for implementing deep learning research schedulers. Blox provides modular abstractions which can be easily swapped/modified to enable researchers to implement novel scheduling and placement policies.

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

### Writing a simulator in Blox

For implementing a new scheduler in Blox, a user first needs to determine in what part of the scheduler do they want to modify. 

Once the user has determined the specific location of their contribution. They can look at the following guide to determine, what code do they need to modidy. 
- Following is the location of files - 
- Scheduling Policy - /schedulers
- Placement Policy - /placement
- Workload Policy - /workload
- :Admission Policy - /admission_control


For an example users should look at `las_scheduler.py` which implements Least Attained Service scheduler.

### Running Blox

#### Installation
Blox uses gRpc, Matplotlib to communicate and Plot several collected Metric.
We suggest the users to create a virtual environment to install the dependencies.
```
pip install grpcio
pip install matplotlib
pip install pandas==1.3.0
pip install grpcio-tools

pip install protobuf==4.21.1
cd blox/deployment
mkdir grpc_stubs
make
```

#### Prepare the trace

Take philly trace as an example, download the trace from [here](https://github.com/msr-fiddle/philly-traces/blob/master/trace-data.tar.gz) and unpack it.

You can get a file named `job_cluster_log` which will be used in the following examples.

Blox has two modes for running. One real cluster workload and second simulator. 

##### Simulation Mode

The simplest way to get started with Blox is in simulation mode. 

The following code will run LAS scheduling policy in simulation mode on the Philly Trace with jobs sent with load average of 1.0

On one terminal launch - 

```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_simple.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 1 --exp-prefix test
```

On the second terminal launch - 

```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python las_scheduler.py --simulate --load 1 --exp-prefix test

```

Make sure only one instance of each is running on a machine/docker container. We use fixed GRPC ports to communicate, if more than one are launched there could be some unintended consequences.


##### Cluster mode
On the node where we are running the scheduler

```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python las_scheduler.py --expe-prefix cluster_run
```

On each node in the cluster launch 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python node_manager.py --ipaddr ip_address_scheduler
 ```
In certain cases you would want to specifically give the interface you want the node manager to bind. For ex- on AWS to bind to the local ip for communication you might want to select eth0, similarly on Cloudlab the preferred interface will be enp94s0f0.
In those cases launch node_manager in the following way. 

```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python node_manager.py --ipaddr ip_address_scheduler --interface interface_name
```


### Details for reproducing results for artifacts
These are instructions for reproducing artifacts for Blox.

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



Replicating only Figure 12

In one terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_acceptance_policy.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```
In second terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python blox_new_flow_multi_run.py --simulate --load 6 --exp-prefix test
```



For running LAS scheduler with different acceptance policy. This will provide Avg JCTs for Figure 12 and Figure 13.
In one terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python simulator_dual_load.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 6 --exp-prefix test
```
In second terminal 
```
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python blox_new_flow_multi_run.py --simulate --load 6 --exp-prefix test
```


#### Running Multiple Solutions in Parallel 
Blox supports running multiple simulations on the same machine. The authors will need to specify the port numbers correctly. 
Here is an example to run multiple simulations. Run the following commands in different terminals. 

Running First simulation.
```
python simulator_simple.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 1 --exp-prefix test --simulator-rpc-port 50511
```
```
python las_scheduler.py --simulate --load 1 --exp-prefix test --simulator-rpc-port 50511
```

Running Second Simulation.
```
python simulator_simple.py --cluster-job-log ./cluster_job_log --sim-type trace-synthetic --jobs-per-hour 1 --exp-prefix test --simulator-rpc-port 50501
```
```
python las_scheduler.py --simulate --load 1 --exp-prefix test --simulator-rpc-port 50501
```
#### Running cluster experiments in Blox
Blox allows users to run experiments on the real cluster. However, running experiments on real cluster requires additional setup.

First on a node launch the scheduler you will like to run. For example - 
```
python las_scheduler_cluster.py --round-duration 30 --start-id-track 0 --stop-id-track 10
```
The above command will launch the cluster scheduler. 

Next on each node which you plan to run the jobs on, start redis-server and launch the job-manager.
```
redis-server
```
Post starting redis-server, you need to launch the node manager.

```
python node_manager.py --ipaddr {ip_addr_of_scheduler} --interface {network_interface_to_use}
```
For starting the node manager, we need two mandatory arguments, the IP Address of the scheduler and the network interface you want to use the node manager to use. 
To get the network interface you can run `ip a`.

Finally you need to send the jobs to the the scheduler. For an example of how to submit jobs you can look [here](https://github.com/msr-fiddle/blox/blob/main/blox/deployment/job_submit_script.py).

To launch a job on the cluster there are two mandatory fields to blox.
First is the launch_command, launch_command gives the command to launch.
Next you can pass any command line arguments with Blox, by using the launch_params key, as done in the job_submit_script.py.
For each application blox also sets following environment variables. 
For multiple jobs blox will launch the command multiple times. However, with differnt environment variables.
The users are responsible to configure the environment variables themselves. Following is the enviroment variable to query, and their description. 
```
local_gpu_id : specifies the local gpu id this job is running
master_ip_address: in case of a distributed job this is the master-ip-address to use.
world_size: total number of workers running this command
dist_rank: rank of the current process
job_id: job id assigned to this particular job by blox
local_accessible_gpu: all gpus that can accessed by this job. This is especially useful with CUDA_VISIBLE_DEVICES.
``` 
