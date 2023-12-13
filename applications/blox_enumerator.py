# grpc
from blox.deployment import grpc_client_blox_iterator as bloxComm


class bloxEnumerate(enumerate):
    def __init__(self, x, jid, start=0):
        self.x_ = x
        self.len_ = len(x)
        self._rpc_client = bloxComm.BloxIteratorComm(jid)

    def __next__(self):
        # check lease
        out = super().__next__()
        # fake iteration number for now
        status = self._rpc_client.check_lease(1)
        return out, status

    def push_metrics(self, metric_dict):
        self._rpc_client.push_metrics(metric_dict)

    def job_exit_notify(self):
        print("Called exit")
        self._rpc_client.job_exity_notify()


if __name__ == "__main__":
    # Example usage:
    for ictr, key in bloxEnumerate(["a", "b", "c"]):
        pass
