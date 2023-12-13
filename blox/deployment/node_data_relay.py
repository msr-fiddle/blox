# handles node data, like per iteration time lease etc
class DataRelay(object):
    def __init__(self, **kwargs):
        if kwargs.get("use_redis") == True:
            import redis

            self.use_redis = True
            self.redis_client = redis.redis(
                host=kwargs["redis_host"], port=kwargs["redis_port"]
            )
        else:
            self.use_redis = None
            self.data_dict = dict()
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
        key_to_search = "{}_lease".format(job_id)
        key_to_iteration = "{}_iteration".format(job_id)
        if self.use_redis:
            lease_status = self.redis_client.get(key_to_search)
            self.redis_client.set(key_to_set, iteration)
        else:
            lease_status = self.data_dict[key_to_search]
            self.data_dict[key_to_iteration] = iteration

        return lease_status

    def set_lease_status(self, job_id: int, status: bool) -> None:
        """
        Set lease status for a given job_id
        Args:
            job_id: The job ID of the associated job
            status : Status of the lease for the associated job_id
        """
        key_to_set = "{}_lease".format(job_id)
        if self.use_redis:
            self.redis_client.set(key_to_set, status)
        else:
            self.data_dict[key_to_set] = status

    def reset_job_metrics(self, job_id: int):
        """
        Reset Job Metrics.

        We usually call it after reading job status everytime.
        Args:
            job_id: The job ID of the associated job
        """
        if self.use_redis:
            pass
        else:
            if job_id in self.data_dict:
                del self.data_dict[job_id]
            else:
                pass
        return None

    def get_job_metrics(self, job_id: int) -> dict:
        """
        Sets the jobs metrics
        """
        if self.use_redis:
            # TODO: Handle this for a key which doesn't exist
            data = self.redis_client.get(job_id)
            data = json.loads(data)
        else:
            if job_id in self.data_dict:
                data = self.data_dict[job_id]
            else:
                data = {}
        # print("get_job_metrics data dict {}".format(self.data_dict))
        return data

    def set_job_metrics(self, job_id: int, metrics: dict) -> None:
        """
        Set the metrics pushed by job.
        """
        # TODO: Add custom parsing scripts

        # TODO: Get rid of this, bad hack for implementing LAS
        # if "attained_service" not in self.data_dict[job_id]:
        # metrics["attained_service"] = metrics["per_iter_time"]
        # else:
        # metrics["attained_service"] += metrics["per_iter_time"]
        if self.use_redis:
            # TODO: For redis handle dictionary update parameter
            self.redis_client.set(job_id, json.dumps(metrics))
        else:
            # metrics are comming ready to push
            # TODO: Get rid of bad hack for job
            # already getting
            if job_id not in self.data_dict:
                self.data_dict[job_id] = metrics
            # if "per_iter_time" in metrics:
            # metrics["attained_service"] = metrics["per_iter_time"]
            # self.data_dict[job_id] = metrics
            # else:
            # if "per_iter_time" in metrics:
            # metrics["attained_service"] = (
            # self.data_dict[job_id]["attained_service"]
            # + metrics["per_iter_time"]
            # )
            else:
                self.data_dict[job_id].update(metrics)
        # print("set job metrics data dict {}".format(self.data_dict))
        print("Self Data dict {}".format(self.data_dict))
        return None

    def get_rm_metrics(self, job_id: int, metric_to_fetch: str) -> dict:
        """
        Returns the metric requested by job id
        """
        key_to_read = "{}_rm_metrics".format(job_id)
        if self.use_redis:
            data = self.redis_client.get(key_to_read)
            data = json.loads(data)
            data = data[metric_to_fetch]
        else:
            data = self.data_dict[key_to_read][metric_to_fetch]
        return data

    def set_rm_metrics(self, job_id: int, metrics: dict) -> None:
        """
        Set the metrics pushed by job id
        """
        key_to_write = "{}_rm_metrics".format(job_id)
        if self.use_redis:
            self.redis_client.set(key_to_write, json.dumps(metrics))
        else:
            if key_to_write not in self.data_dict:
                self.data_dict[key_to_write] = metrics
            else:
                self.data_dict[key_to_write].update(metrics)
        return None
