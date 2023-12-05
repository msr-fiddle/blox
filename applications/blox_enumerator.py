# grpc
from blox.deployment import grpc_client_blox_iterator as bloxComm


class bloxEnumerate(enumerate):
    def __init__(self, x, start=0):
        self.x_ = x
        self.len_ = len(x)
        self._rpc_client = bloxComm.BloxIterator()

    def __next__(self):
        # check lease
        out = super().__next__()
        status = self._rpc_client.check_lease()
        return out, status


if __name__ == "__main__":
    # Example usage:
    for ictr, key in bloxEnumerate(["a", "b", "c"]):
        pass
