import queue
from .admission_policy import AdmissionControl


class loadBasedAccept(AdmissionControl):
    def __init__(self, args, load_thresh):
        # self.overflow_queue = queue.Queue()
        self.overflow_queue = list()
        self.load_thresh = load_thresh
        self.simulator_time = 0

    def admit_old(self, new_jobs, cluster_state, job_state):
        number_of_gpus = get_num_gpus(cluster_state)
        current_gpu_demand = get_current_gpu_demand(job_state)
        for job in new_jobs:
            self.overflow_queue.put(job)
        new_job_gpu_demand = 0
        jobs_to_admit = list()
        while (
            current_gpu_demand + new_job_gpu_demand <= self.load_thresh * number_of_gpus
        ):
            if self.overflow_queue.qsize() > 0:
                new_job = self.overflow_queue.get()
                new_job_gpu_demand += new_job["num_GPUs"]
                jobs_to_admit.append(new_job)
            else:
                break
        return jobs_to_admit

    # @AdmissionControl.copy_arguments
    # removing deep copy
    def accept(self, new_jobs, cluster_state, job_state, **kwargs):
        """ """
        number_of_gpus = get_num_gpus(cluster_state)
        current_gpu_demand = get_current_gpu_demand(job_state)
        for _ in range(len(new_jobs)):
            job = new_jobs.pop(0)
            self.overflow_queue.append(job)
        new_job_gpu_demand = 0
        jobs_to_admit = list()
        while (
            current_gpu_demand + new_job_gpu_demand <= self.load_thresh * number_of_gpus
        ):
            if len(self.overflow_queue) > 0:
                new_job = self.overflow_queue.pop(0)
                new_job["wait_queue_pop"] = self.simulator_time
                new_job_gpu_demand += new_job["num_GPUs"]
                jobs_to_admit.append(new_job)
            else:
                break
        print("Size of wait queue {}".format(len(self.overflow_queue)))
        return jobs_to_admit


def get_current_gpu_demand(job_state):
    gpu_demand = 0
    for jid in job_state.active_jobs:
        gpu_demand += job_state.active_jobs[jid]["num_GPUs"]
    return gpu_demand


def get_num_gpus(cluster_state):
    """
    Get number of GPUs
    """
    return len(cluster_state.gpu_df)
