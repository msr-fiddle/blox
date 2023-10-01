class acceptAll(object):
    def __init__(self, args):
        self.simulator_time = 0

    def accept(self, new_jobs, cluster_state, job_state, **kwargs):
        return new_jobs
