from .scheduler_policy import SchedulingPolicy
import pandas as pd
from copy import deepcopy
from operator import getitem

from typing import Optional

from policy import Policy, PolicyWithPacking
from proportional import ProportionalPolicy


class Gavel(SchedulingPolicy):
    """
    Implement Gavels Maxmin Fairness Policy1
    """

    def __init__(self, args, **kwargs):
        """ """

        self.metric_to_track = ["per_iter_time", "attained_service"]
        self.default_metric_value = [0, 0]
        try:
            self.solver = kwargs["solver"]
        except KeyError:
            print("Solver not provided to Gavel")
        self._max_min_fairness_perf_policy = MaxMinFairnessPolicyWithPerf(self.solver)

    def schedule(
        self,
        job_dict: dict,
        node_info: dict,
        gpu_df: pd.DataFrame,
        global_placement_policy: Optional[str] = None,
    ) -> dict:

        # mostly copied from gavel policies/max_min_fairness.py

        scale_factors = dict()
        priority_weights = dict()
        unflattened_throughputs = dict()

        cluster_spec = gpu_df.GPU_type.value_counts().to_dict()

        for jid in job_dict.keys():
            job_throughputs[jid] = job_dict[jid]["throughput"]
            scale_factors[jid] = job_dict[jid]["job_gpu_demand"]
            priority_weights[jid] = job_dict[jid]["priority_weights"]

        new_unflattened_throughput = dict()

        for job_id in unflattened_throughputs:
            new_unflattened_throughputs[job_id] = {}
            for worker_type in unflattened_throughputs[job_id]:
                new_unflattened_throughputs[job_id][worker_type] = 1.0

        allocation_gavel = self._max_min_fairness_perf_policy.get_allocation(
            new_unflattened_throughput, scale_factors, priority_weights, cluster_spec
        )
        return allocation_gavel


class MaxMinFairnessPolicyWithPerf(Policy):
    def __init__(self, solver):
        Policy.__init__(self, solver)
        self._name = "MaxMinFairness_Perf"
        self._proportional_policy = ProportionalPolicy()

    def get_allocation(
        self,
        unflattened_throughputs,
        scale_factors,
        unflattened_priority_weights,
        cluster_spec,
    ):
        throughputs, index = super().flatten(unflattened_throughputs, cluster_spec)
        if throughputs is None:
            return None
        (m, n) = throughputs.shape
        (job_ids, worker_types) = index

        # Row i of scale_factors_array is the scale_factor of job i
        # repeated len(worker_types) times.
        scale_factors_array = self.scale_factors_array(scale_factors, job_ids, m, n)

        priority_weights = np.array(
            [1.0 / unflattened_priority_weights[job_id] for job_id in job_ids]
        )

        proportional_throughputs = self._proportional_policy.get_throughputs(
            throughputs, index, cluster_spec
        )
        priority_weights = np.multiply(
            priority_weights.reshape((m, 1)),
            1.0 / proportional_throughputs.reshape((m, 1)),
        )

        x = cp.Variable(throughputs.shape)
        # Multiply throughputs by scale_factors to ensure that scale_factor
        # is taken into account while allocating times to different jobs.
        # A job run on 1 GPU should receive `scale_factor` more time than
        # a job run on `scale_factor` GPUs if throughputs are equal.
        objective = cp.Maximize(
            cp.min(
                cp.sum(
                    cp.multiply(
                        np.multiply(
                            throughputs * priority_weights.reshape((m, 1)),
                            scale_factors_array,
                        ),
                        x,
                    ),
                    axis=1,
                )
            )
        )
        # Make sure that the allocation can fit in the cluster.
        constraints = self.get_base_constraints(x, scale_factors_array)
        cvxprob = cp.Problem(objective, constraints)
        result = cvxprob.solve(solver=self._solver)

        if cvxprob.status != "optimal":
            print("WARNING: Allocation returned by policy not optimal!")

        return super().unflatten(x.value.clip(min=0.0).clip(max=1.0), index)
