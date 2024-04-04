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

    def get_lease_status_rank0(self, job_id: int, iteration: int) -> bool:
        """
        For a given id check job lease
        Args:
            job_id: Job ID associated
            iteration: Iteration number of the job
        """
        lease_key = f"{job_id}_lease_rank0"
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

    def get_peer_ipaddress_rank0(self, job_id: int) -> List[str]:
        key_to_read = f"{job_id}_terminate_ips"
        if self.use_redis:
            ipaddress = self.redis_client.get(key_to_read)
            ipaddress = json.loads(ipaddress)
        return ipaddress

    def set_lease_status_rank0(
        self, job_id: int, ipaddr_to_terminate: List[str], status: bool
    ) -> None:
        """
        Set lease status for a given job_id
        Args:
            job_id: The job ID of the associated job
            status : Status of the lease for the associated job_id
        """
        key_to_set = f"{job_id}_lease_rank0"
        key_to_set_ipaddr = f"{job_id}_terminate_ips"
        if self.use_redis:
            status = str(status)
            print("Setting status {} job id {}".format(status, job_id))
            self.redis_client.set(key_to_set, status)
            self.redis_client.set(key_to_set_ipaddr, json.dumps(ipaddr_to_terminate))
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

    def push_terminated_jobs(self, job_id: int) -> None:
        """
        Push jobs to terminate
        """

        out = self.redis_client.rpush("terminated_jobs", job_id)
        return out

    def get_terminated_jobs(self) -> List[str]:
        """
        Return terminated job list
        """
        terminated_job_list = self.redis_client.lrange("terminated_jobs", 0, -1)
        return terminated_job_list

    def get_jobs_to_terminate(self) -> None:
        """
        Return jobs to terminate
        """

        all_job_to_terminate = self.redis_client.lrange("all_job_terminate_list", 0, -1)
        return all_job_to_terminate

    def get_job_ids_to_check_terminate(self) -> None:
        """
        Get Job IDs to terminate
        """
        jid = self.redis_client.lpop("job_check_list")
        return jid

    def delete_all_job_terminate_list(self) -> None:
        """
        Delete the job terminate list list
        """
        self.redis_client.delete("terminated_jobs")
        self.redis_client.delete("all_job_terminate_list")
        return None

    def set_lease_status_rank0_batch_false(
        self, job_id_list: List[int], corresponding_ip_list: List[int]
    ) -> None:
        """
        Pushes in a batch
        """
        with self.redis_client.pipeline() as redis_pipe:
            for job_id, corresponding_ips in zip(job_id_list, corresponding_ip_list):
                redis_pipe.rpush("all_job_terminate_list", job_id)
                redis_pipe.rpush("job_check_list", job_id)
                key_to_set = f"{job_id}_lease_rank0"
                key_to_set_ipaddr = f"{job_id}_terminate_ips"
                status = str(False)
                redis_pipe.set(key_to_set, status)
                redis_pipe.set(key_to_set_ipaddr, json.dumps(corresponding_ips))
                print("Setting status {} job id {}".format(status, job_id))
            redis_pipe.execute()
        return None

    def push_job_to_terminate(self, job_id: int) -> None:
        """
        This for pushing jobs to terminate.
        Writes two lists, "all_job_terminate_list" and "job_check_list".
        The two of those are same uptil a point.
        """
        with self.redis_client.pipeline() as redis_pipe:
            self.redis_client.rpush("all_job_terminate_list", job_id)
            self.redis_client.rpush("job_check_list", job_id)
            redis_pipe.execute()

        return None

    def set_job_metrics_float(self, job_id: int, metrics: dict) -> None:
        """
        Set Job Metrics for float increment
        """
        metric_key = f"{job_id}_metrics"
        with self.redis_client.pipeline() as redis_pipe:
            for key in metrics:
                redis_pipe.hincrbyfloat(metric_key, key, metrics[key])
            updated_value = redis_pipe.execute()
            print("Input Metrics {}".format(metrics))
            print("Updated Metrics {}".format(updated_value))
        return None

    def set_job_metrics_watch(self, job_id: int, metrics: dict) -> None:
        """
        Update a key by puting a watch a command
        """
        metric_key = f"{job_id}_metrics"
        with self.redis_client.pipeline() as redis_pipe:
            while True:
                try:
                    redis_pipe.watch(metric_key)
                    previous_metrics = redis_pipe.hgetall(metric_key)
                    for key_metric in data:
                        data[key_metric] = self.data_type[key_metric](data[key_metric])
                    redis_pipe.multi()
                    for key in metrics:
                        if key == "per_iter_time":
                            if key in previous_metrics:
                                metrics[key] = (
                                    metrics[key] + previous_metrics[key]
                                ) / 2
                            else:
                                pass
                            redis_pipe.hset(metric_key, key, metrics[key])
                    redis_pipe.execute()
                except redis.WatchError:
                    continue

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

    def get_job_status(self, job_id: int) -> None:
        """
        Get Job Status
        """
        status_key = f"{job_id}_status"
        get_data = self.redis_client.get(status_key)
        return get_data

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
