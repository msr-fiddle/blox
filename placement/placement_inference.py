import pandas as pd

from typing import Tuple, List


class JobPlacementInference(object):
    def __init__(self):
        self.prev_jobs = list()
        return None

    def place_inference_jobs(
        self,
        job_id: int,
        jobs_description: dict,
        local_gpu_ids: List[int],
        ipaddr_list: List[str],
    ) -> None:
        """
        Place Inference jobs for training
        """
        pass
