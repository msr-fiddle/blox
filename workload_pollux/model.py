import os
from models import *

# from models import ModelStats, alexnet_1
""" Model characteristics
     - model_packing_score (1xn) matrix - preference of packing this model against all other 
            available models
     - model_placement_score (1x2) matrix - job slowdown for consolidation vs no-consolidation
     - synergy_res_score (c x m) matrix - job speeds for different combinations of cpu/mem
           TODO : How does this vary with no-consolidation?
           - should we simply multiply the slowdown from no-consolidation to this matrix?

     - synergy_storage_score (m x 1) matrix - storage tput requiurement for the job for varying
           memory allocations

     - model class (image/language/speech)
     - model ID (unique ID for the model)
"""


class Model:
    def __init__(self, name, task, model_id, gpu=1):
        self.model_name = name
        self.batch_size = 512
        self.model_id = model_id
        self.model_task = task
        self.model_packing_score = dict()
        self.model_placement_score = list()
        self.synergy_res_score = dict()
        self.synergy_storage_score = dict()
        self.total_jobs = 0
        self.runnable_jobs = 0
        self.gpu_demand = gpu
        # Model iteration time for single GPU
        # assuming fair share
        self.iteration_time = 0
        self.tput = None
        # self.placement_penalty = 1
        # Speedup in iteration time for synergy
        # share compared to fair share
        # Ideally must be >= 1 for speedup
        # < 1 means slowdown.
        # Infer it from matrix. For now, hardcode
        self.speedup = 1

        # These are for analysis assuming the
        # full profiling matrix is unavailable
        self.cpu_per_gpu = 3
        self.mem_per_gpu = 62.5
        self.sspeed_per_gpu = 62.5

    def __str__(self):
        return "{}:{}:{}".format(self.model_name, self.cpu_per_gpu, self.mem_per_gpu)

    """
    Fill in synergy profile scores from the json generated by the
    Synergy Profiler
    """

    def update_res_score_from_json(self, fname, score_type="cpu-mem"):
        if score_type == "cpu-mem":
            score_to_update = self.synergy_res_score
        else:
            score_to_update = self.synergy_storage_score

        try:
            with open(fname) as json_file:
                score_to_update = json.load(json_file)
        except:
            raise ("Could not parse synergy profile")

    """
    Fair share scores for * GPU-24CPU-500GB-500MB/s
    """

    def use_default_scores(self):
        DEFAULT_CPU = 3
        DEFAULT_MEM = 500 / 8
        DEFAULT_SSPEED = 500 / 8

        self.cpu_per_gpu = DEFAULT_CPU
        self.mem_per_gpu = DEFAULT_MEM
        self.sspeed_per_gpu = DEFAULT_SSPEED

    """
    Some hardcoded scores for analysis, if profiling matrix is unavailable
    """

    def use_approx_scores(self):
        DEFAULT_CPU = 3
        DEFAULT_MEM = 500 / 8
        DEFAULT_SSPEED = 500 / 8

        if "res18" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 3
            self.mem_per_gpu = DEFAULT_MEM * 2
            self.sspeed_per_gpu = DEFAULT_SSPEED * 2
            self.iteration_time = 1
            self.speedup = 1.8
            self.placement_penalty = 0.8
            # self.speedup = 3
        elif "res50" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            # self.mem_per_gpu = DEFAULT_MEM*2
            # self.sspeed_per_gpu = DEFAULT_SSPEED*2
            self.iteration_time = 1
            self.speedup = 1
            self.placement_penalty = 0.9
        elif "alexnet" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 4
            self.mem_per_gpu = DEFAULT_MEM * 2
            self.sspeed_per_gpu = DEFAULT_SSPEED * 2
            self.iteration_time = 1
            self.speedup = 3
            self.placement_penalty = 0.6
            # self.speedup = 4
        elif "mobile" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 2
            self.mem_per_gpu = DEFAULT_MEM * 2
            self.sspeed_per_gpu = DEFAULT_SSPEED * 2
            self.iteration_time = 1
            self.speedup = 1.5
            self.placement_penalty = 0.8
            # self.speedup = 2
        elif "ssd" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 2
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 1
            self.speedup = 1.5
            self.placement_penalty = 0.8
            # self.speedup = 2
        elif "shuffle" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 4
            self.mem_per_gpu = DEFAULT_MEM * 2
            self.sspeed_per_gpu = DEFAULT_SSPEED * 2
            self.iteration_time = 1
            self.speedup = 2.5
            self.placement_penalty = 0.7
            # self.speedup = 4
        elif "m5" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 2
            self.mem_per_gpu = DEFAULT_MEM * 3
            self.sspeed_per_gpu = DEFAULT_SSPEED * 3
            self.iteration_time = 1
            self.speedup = 2
            self.placement_penalty = 0.8
            # self.speedup = 3
        elif "deepspeech" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 2
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 1
            self.speedup = 1.5
            self.placement_penalty = 0.8
            # self.speedup = 2
        else:
            self.cpu_per_gpu = DEFAULT_CPU / 3
            self.mem_per_gpu = DEFAULT_MEM / 2
            self.sspeed_per_gpu = DEFAULT_SSPEED / 2
            self.iteration_time = 1
            self.speedup = 1
            self.placement_penalty = 0.9

    def use_scores_from_tput(self):
        modelname = self.model_name + "_" + str(self.gpu_demand)
        if modelname not in globals():
            modelname = self.model_name + "_1"
        model = globals()[modelname](self.model_name)
        self.cpu_per_gpu = model.cpus
        self.mem_per_gpu = model.mem
        self.speed_per_gpu = model.speed
        self.batch_size = model.batch
        self.iteration_time = model.iter_time
        self.speedup = model.speedup
        self.placement_penalty = model.placement_penalty
        self.tput = model.tput

    def use_real_scores(self):
        DEFAULT_CPU = 3
        DEFAULT_MEM = 500 / 8
        DEFAULT_SSPEED = 500 / 8

        # All 1-GPU workloads
        if "res18" in self.model_name:
            # TODO : Debug - use this value for opt
            # self.cpu_per_gpu = DEFAULT_CPU*2
            self.cpu_per_gpu = DEFAULT_CPU * 3
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.batch_size = 512
            self.iteration_time = 0.64
            # self.iteration_time = 0.6
            self.speedup = 2.4
            self.placement_penalty = 1
        elif "res50" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU + 1
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.batch_size = 512
            self.iteration_time = 0.78
            # self.iteration_time = 0.74
            self.speedup = 1.15
            self.placement_penalty = 1
        elif "alexnet" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 4
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 0.6
            # self.iteration_time = 0.56
            self.batch_size = 512
            self.speedup = 2.94
            self.placement_penalty = 1
        elif "mobile" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 2
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 0.71
            # self.iteration_time = 0.67
            self.speedup = 1.55
            self.placement_penalty = 1
            self.batch_size = 512
        elif "shuffle" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 4
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 0.615
            # self.iteration_time = 0.57
            self.speedup = 2.89
            self.batch_size = 512
            self.placement_penalty = 1
        elif "deepspeech" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 0.757
            # self.iteration_time = 0.757
            self.speedup = 1
            self.batch_size = 20
            self.placement_penalty = 1
        elif "m5" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU * 2
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 0.486
            # self.iteration_time = 0.265
            self.speedup = 1
            self.batch_size = 32
            self.placement_penalty = 1
        elif "transformer" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU / 3
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 0.67
            self.batch_size = 128
            self.speedup = 1
            self.placement_penalty = 1
        elif "gnmt" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU / 3
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 0.12
            self.speedup = 1
            self.placement_penalty = 1
            self.batch_size = 128
        elif "lstm" in self.model_name:
            self.cpu_per_gpu = DEFAULT_CPU / 3
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 0.011
            self.speedup = 1
            self.placement_penalty = 1
            self.batch_size = 20
        else:
            self.cpu_per_gpu = DEFAULT_CPU
            self.mem_per_gpu = DEFAULT_MEM
            self.sspeed_per_gpu = DEFAULT_SSPEED
            self.iteration_time = 1
            self.speedup = 1
            self.placement_penalty = 1
            self.batch_size = 512