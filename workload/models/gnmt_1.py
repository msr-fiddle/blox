from models.model_stats import ModelStats
import numpy as np

class gnmt_1(ModelStats):
    def __init__(
        self,
        name):
      super().__init__(name, 1)
 
      
    def update_stats(self):
        self.cpus = 1
        self.mem = 62.5
        self.speed = 62.5
        self.batch = 128
        self.placement_penalty = 1
        self.speedup = 1
        self.iter_time = 0.12
        self.tput = np.array([
             [1,1,1,1],
             [1,1,1,1],
             [1,1,1,1],
             [1,1,1,1],
             [1,1,1,1],
             [1,1,1,1],
             [1,1,1,1],
             [1,1,1,1],
             [1,1,1,1]
             ])

                             
      

     
      

     
