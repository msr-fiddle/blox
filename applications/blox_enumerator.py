# grpc
from blox.deployment import grpc_client_blox_iterator as bloxComm


class bloxEnumerate(object):
    def __init__(self, x, jid, rank, start=0):
        """
        x: Number of iterations
        jid: Job ID
        rank: Rank distributed
        """
        self.x_ = list(x)
        self.len_ = len(x)
        self._rpc_client = bloxComm.BloxIteratorComm(jid, rank)

    def __next__(self):
        # check lease
        out = self.x_.pop()
        # fake iteration number for now
        status = self._rpc_client.check_lease(1)
        return out, status

    def push_metrics(self, metric_dict):
        self._rpc_client.push_metrics(metric_dict)

    def job_exit_notify(self):
        print("Called exit")
        self._rpc_client.job_exit_notify()


if __name__ == "__main__":
    # Example usage:
    for ictr, key in bloxEnumerate(["a", "b", "c"]):
        pass
