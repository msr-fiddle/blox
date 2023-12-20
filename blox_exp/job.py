import pandas as pd

class Job:
    def __init__(
        self,
        job_id,
        job_arrival_time,
        job_iteration_time,
        job_total_iteration,
        job_gpu_demand,
        job_model,
        batch_size
    ):
        # job details
        self.job_id = job_id
        self.job_arrival_time = job_arrival_time
        self.job_iteration_time = job_iteration_time
        self.job_total_iteration = job_total_iteration
        self.job_duration = job_iteration_time * job_total_iteration
        self.job_gpu_demand = job_gpu_demand
        self.job_model = job_model
        self.batch_size = batch_size

        self.job_gpu_demand_orig = job_gpu_demand
        self.job_iteration_time_orig = job_iteration_time

        # colocate information
        # {f"{mdl}_{gpu_num}_{bs}": (this.speed, colocate.speed)}
        self.small_colocate_dict = {}
        # {f"{mdl}_{gpu_num}_{bs}": [('pipeline_strategy', 'co_pipeline_rank', large_model_speed, small_model_speed)]}
        self.large_colocate_dict = {}
        self.small_model_list = ['resnet50', 'vgg19', 'DCGAN', 'PointNet']
        self.large_model_list = ['Bert-Large', 'GPT2-Medium']
        self.init_colocate_info()

        # job state
        self.gpus = list()
        self.num_allocated_gpus = 0
        self.gpus_last_round = list()
        self.job_executed_iteration = 0
        self.job_last_execution_time = -1
        self.attained_service_time = 0
        
        # command to run jobs on nersc cluster
        self.launch_command = ""
        self.launch_params = []
        self.init_launch()

    def __eq__(self, other):
        return self.job_id == other.job_id

    def __str__(self):
        return "job:%s:%s:%s (%s s,%s)" % (
            self.job_id, self.job_model, self.job_gpu_demand, self.job_arrival_time, self.job_total_iteration
        )
    
    def init_colocate_info(self):
        colocate_df = pd.read_csv(
            filepath_or_buffer="/global/homes/s/songbian/Megatron-Resource/blox_exp/colocate_info/norm_speed.csv",
            dtype={"model1": str, "model2": str,
                   "batch_size1": str, "batch_size2": str,
                   "gpu_num1": int, "gpu_num2": int,
                   "speed1": float, "speed2": float,
                   "co_pipeline_rank": str, "pipeline_strategy": str}
        )

        if self.job_model in self.large_model_list:
            for _, value in colocate_df.iterrows():
                if value.model2 == "Empty":
                    pass
                else:
                    if (value.model1 == self.job_model
                            and value.gpu_num1 == self.job_gpu_demand
                            and value.model2 in self.small_model_list):
                        if (f"{value.model2}_{value.gpu_num2}_{value.batch_size2}"
                                not in self.large_colocate_dict.keys()):
                            self.large_colocate_dict[
                                f"{value.model2}_{value.gpu_num2}_{value.batch_size2}"
                            ] = []
                        self.large_colocate_dict[
                            f"{value.model2}_{value.gpu_num2}_{value.batch_size2}"
                        ].append(
                            (value.pipeline_strategy, value.co_pipeline_rank, value.speed1, value.speed2)
                        )

        elif self.job_model in self.small_model_list:
            for _, value in colocate_df.iterrows():
                if value.model2 == "Empty":
                    pass
                else:
                    if (value.model1 == self.job_model
                            and value.gpu_num1 == self.job_gpu_demand
                            and value.batch_size1 == self.batch_size
                            and value.model2 in self.small_model_list):
                        self.small_colocate_dict[
                            f"{value.model2}_{value.gpu_num2}_{value.batch_size2}"
                        ] = (value.speed1, value.speed2)
                    elif (value.model2 == self.job_model
                            and value.gpu_num2 == self.job_gpu_demand
                            and value.batch_size2 == self.batch_size
                            and value.model1 in self.small_model_list):
                        self.small_colocate_dict[
                            f"{value.model1}_{value.gpu_num1}_{value.batch_size1}"
                        ] = (value.speed2, value.speed1)
                    elif (value.model2 == self.job_model
                            and value.gpu_num2 == self.job_gpu_demand
                            and value.batch_size2 == self.batch_size
                            and value.model1 in self.large_model_list):
                        if f"{value.model1}_{value.gpu_num1}_NA" not in self.large_colocate_dict.keys():
                            self.large_colocate_dict[
                                f"{value.model1}_{value.gpu_num1}_NA"
                            ] = []
                        self.large_colocate_dict[
                            f"{value.model1}_{value.gpu_num1}_NA"
                        ].append(
                            (value.pipeline_strategy, value.co_pipeline_rank, value.speed1, value.speed2)
                        )
        
        else:
            raise ValueError("the job model is not considered now!!!")

    
    def init_launch(self):
        default_placement = ''
        for i in range(self.job_gpu_demand):
            default_placement += str(i) + ","
        if self.job_model == "Bert-Large":
            if self.job_gpu_demand == 2:
                split_strategy = "12,12"
            elif self.job_gpu_demand == 4:
                split_strategy = "6,6,6,6"
            else:
                raise ValueError("the job gpu demand is not considered now!!!")
            
            self.launch_command = "bash /global/homes/s/songbian/Megatron-Resource/blox_exp/scripts/run_bert.sh"
            self.launch_params = [
                default_placement, 
                str(self.job_gpu_demand),
                str(6000 + self.job_id), 
                str(self.job_gpu_demand),
                split_strategy,
                self.job_id
            ]
        elif self.job_model == "GPT2-Medium":
            if self.job_gpu_demand == 2:
                split_strategy = "12,12"
            elif self.job_gpu_demand == 4:
                split_strategy = "6,6,6,6"
            else:
                raise ValueError("the job gpu demand is not considered now!!!")
            
            self.launch_command = "bash /global/homes/s/songbian/Megatron-Resource/blox_exp/scripts/run_gpt.sh"
            self.launch_params = [
                default_placement, 
                str(self.job_gpu_demand),
                str(6000 + self.job_id), 
                str(self.job_gpu_demand),
                split_strategy,
                self.job_id
            ]
        elif self.job_model == "resnet50" or self.job_model == "vgg19":
            self.launch_command = "bash /global/homes/s/songbian/Megatron-Resource/blox_exp/scripts/run_imagetnet.sh"
            self.launch_params = [
                default_placement, 
                str(6000 + self.job_id), 
                str(self.job_gpu_demand),
                self.job_model,
                self.batch_size,
                self.job_id
            ]
        elif self.job_model == "DCGAN":
            self.launch_command = "bash /global/homes/s/songbian/Megatron-Resource/blox_exp/scripts/run_dcgan.sh"
            self.launch_params = [
                default_placement, 
                str(6000 + self.job_id), 
                str(self.job_gpu_demand),
                self.batch_size,
                self.job_id
            ]
        elif self.job_model == "PointNet":
            self.launch_command = "bash /global/homes/s/songbian/Megatron-Resource/blox_exp/scripts/run_pointnet.sh"
            self.launch_params = [
                default_placement, 
                str(6000 + self.job_id), 
                str(self.job_gpu_demand),
                self.batch_size,
                self.job_id
            ]
        else:
            raise ValueError("the model is not considered now!!!")