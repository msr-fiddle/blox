import sys
from blox_enumerator import bloxEnumerate

run_times = 2
enumerator = bloxEnumerate(range(2))
for ictr, key in enumerator:
    print(ict, key)
    time.sleep(10)
