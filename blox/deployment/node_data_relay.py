# handles node data, like per iteration time lease etc
import sys
import redis


class DataRelay(object):
    def __init__(self, **kwargs):
        # if kwargs.get("use_redis") == True:
        self.use_redis = True
        self.redis_client = redis.Redis(
            host=kwargs["redis_host"], port=kwargs["redis_port"], decode_responses=True
        )
        # datatype mapping
        self.data_type = {
            "attained_service": float,
            "per_iter_time": float,
            "iter_num": int,
        }

    # else:
    # self.use_redis = None
    # self.data_dict = dict()
    # TODO: Remove after debugging
    # self.data_dict["1_lease"] = True

    def get_lease_status(self, job_id: int, iteration: int) -> bool:
        """
        For a given ID check job lease
        Args:
            job_id: The job ID of the associated job
            iteration: Iteration number of the job to handle the training time
        Returns:
            lease_status: True if Lease active, False if not
        """
        # print(f"{self.data_dict}, in data relay")
        lease_key = f"{job_id}_lease"
        iteration_key = f"{job_id}_iteration"
        # if self.use_redis:
        lease_status = self.redis_client.get(lease_key)
        print(f"b Lease status {lease_status}")
        # bad idea but need to fix
        lease_status = eval(lease_status)
        self.redis_client.set(iteration_key, iteration)
        print(f"a lease_status {lease_status}")
        return lease_status

    def set_lease_status(self, job_id: int, status: bool) -> None:
        """
        Set lease status for a given job_id
        Args:
            job_id: The job ID of the associated job
            status : Status of the lease for the associated job_id
        """
        key_to_set = f"{job_id}_lease"
        if self.use_redis:
            status = str(status)
            print("Setting status {} job id {}".format(status, job_id))
            self.redis_client.set(key_to_set, status)
        # else:
        # self.data_dict[key_to_set] = status

    def reset_job_metrics(self, job_id: int):
        """
        Reset Job Metrics.

        We usually call it after reading job status everytime.
        Args:
            job_id: The job ID of the associated job
        """
        key_to_delete = f"{job_id}_metrics"
        if self.use_redis:
            del_response = self.redis_client.delete(key_to_delete)
            if del_response == 0:
                print("Non existent key deleted")
                print(f"Key {job_id}_metrics")

        # else:
        # if job_id in self.data_dict:
        # del self.data_dict[job_id]
        # else:
        # pass
        return None

    def get_job_metrics(self, job_id: int) -> dict:
        """
        Sets the jobs metrics
        """
        metric_key = f"{job_id}_metrics"
        if self.use_redis:
            # TODO: Handle this for a key which doesn't exist
            data = self.redis_client.hgetall(metric_key)
            for key_metric in data:
                data[key_metric] = self.data_type[key_metric](data[key_metric])
        # else:
        # if job_id in self.data_dict:
        # data = self.data_dict[job_id]
        # else:
        # data = {}
        # print("get_job_metrics data dict {}".format(self.data_dict))
        return data

    def reset_keys(self, job_id: int) -> None:
        """
        Clear up metric keys at restart
        """
        delete_key = f"{job_id}_metrics"
        del_response = self.redis_client.delete(delete_key)
        if del_response == 0:
            print("Non existent key deleted")
            print(f"Key {job_id}_metrics")
        return None

    def set_job_metrics(self, job_id: int, metrics: dict) -> None:
        """
        Set the metrics pushed by job.
        """
        # TODO: Add custom parsing scripts

        # TODO: Get rid of this, bad hack for implementing LAS
        # if "attaAined_service" not in self.data_dict[job_id]:
        # metrics["attained_service"] = metrics["per_iter_time"]
        # else:
        # metrics["attained_service"] += metrics["per_iter_time"]

        metric_key = f"{job_id}_metrics"
        if self.use_redis:
            # TODO: For redis handle dictionary update parameter
            # self.redis_client.set(job_id, json.dumps(metrics))
            self.redis_client.hset(metric_key, mapping=metrics)
        # else:
        # print("In Else")
        # # metrics are comming ready to push
        # # TODO: Get rid of bad hack for job
        # # already getting
        # if job_id not in self.data_dict:
        # self.data_dict[job_id] = metrics
        # # if "per_iter_time" in metrics:
        # # metrics["attained_service"] = metrics["per_iter_time"]
        # # self.data_dict[job_id] = metrics
        # # else:
        # # if "per_iter_time" in metrics:
        # # metrics["attained_service"] = (
        # # self.data_dict[job_id]["attained_service"]
        # # + metrics["per_iter_time"]
        # # )
        # else:
        # self.data_dict[job_id].update(metrics)
        # print("set job metrics data dict {}".format(self.data_dict))
        return None

    def set_job_status(self, job_id: int, status: str) -> None:
        """
        Set job status
        """
        status_key = f"{job_id}_status"
        self.redis_client.set(status_key, status)
        return None

    # def get_rm_metrics(self, job_id: int, metric_to_fetch: str) -> dict:
    # """
    # Returns the metric requested by job id
    # """
    # key_to_read = "{}_rm_metrics".format(job_id)
    # if self.use_redis:
    # data = self.redis_client.get(key_to_read)
    # data = json.loads(data)
    # data = data[metric_to_fetch]
    # # else:
    # # data = self.data_dict[key_to_read][metric_to_fetch]
    # return data

    # def set_rm_metrics(self, job_id: int, metrics: dict) -> None:
    # """
    # Set the metrics pushed by job id
    # """
    # key_to_write = "{}_rm_metrics".format(job_id)
    # if self.use_redis:
    # self.redis_client.set(key_to_write, json.dumps(metrics))
    # else:
    # if key_to_write not in self.data_dict:
    # self.data_dict[key_to_write] = metrics
    # else:
    # self.data_dict[key_to_write].update(metrics)
    # return None
