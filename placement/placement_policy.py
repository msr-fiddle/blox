import pandas as pd
import copy
from typing import Tuple, List


class JobPlacement(object):
    def __init__(self, args):
        pass

    @staticmethod
    def copy_arguments(function):
        def function_wrapper(
            self, job_state, cluster_state, new_job_schedule, **kwargs
        ):
            return function(
                self,
                copy.deepcopy(job_state.active_jobs),
                copy.deepcopy(new_job_schedule),
                copy.deepcopy(cluster_state.server_map),
                copy.deepcopy(cluster_state.gpu_df),
                **copy.deepcopy(kwargs),
            )

        return function_wrapper

    @copy_arguments.__func__
    def place(
        self,
        active_jobs: dict,
        new_job_schedule: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        **kwargs,
    ) -> dict:
        """
        Implement the placement policy
        """

        raise NotImplementedError()
