import copy


class AdmissionControl(object):
    def __init__(self, args):
        pass

    @staticmethod
    def copy_arguments(function):
        def function_wrapper(job_dict, node_info, gpu_df, **kwargs):
            return function(
                copy.deepcopy(job_dict),
                copy.deepcopy(node_info),
                copy.deepcopy(gpu_df),
                **copy.deepcopy(kwargs)
            )

        return function_wrapper

    @copy_arguments.__func__
    def accept(self, new_jobs, cluster_state, job_state, **kwargs):
        """
        Implement Acceptance Policy
        new_jobs (list): List of new jobs
        cluster_state (class) : Cluster State object
        job_state (class) : Job State Object

        Returns : list of accepted jobs
        """
        raise NotImplementedError()
