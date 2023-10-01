from models.model_stats import ModelStats 
import numpy as np

class alexnet_1(ModelStats):
    def __init__(
        self,
        name):
      super().__init__(name, 1)
 
      
    def update_stats(self):
        self.cpus = 12
        self.mem = 62.5
        self.speed = 62.5
        self.batch = 512
        self.placement_penalty = 1
        self.speedup = 2.94
        self.iter_time = 0.6
        self.tput = np.array([
             [0.3,0.3,0.3,0.3],
             [0.66,0.66,0.66,0.66],
             [1,1,1,1],
             [1.25,1.25,1.25,1.25],
             [1.53,1.53,1.53,1.53],
             [1.75,1.75,1.75,1.75],
             [2.33,2.33,2.33,2.33],
             [2.94,2.94,2.94,2.94],
             [2.94,2.94,2.94,2.94]
             ])

                             
             #[3.83,3.83,3.83, 3.83]
      

     
      

     
