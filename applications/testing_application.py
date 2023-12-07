import sys
from blox_enumerator import bloxEnumerate
import argparse


def main(args):
    run_times = 2
    print("Job ID {}".format(args.jid))
    print("Iterator initilized")
    enumerator = bloxEnumerate(range(2))
    print("Initialized enumerator")
    for ictr, key in enumerator:
        print(ict, key)
        time.sleep(1)


def parse_args(parser):
    """
    parser : argparse.ArgumentParser
    return a parser with arguments
    """
    parser.add_argument(
        "--jid", default="Las", type=str, help="Name of the scheduling strategy"
    )


if __name__ == "__main__":
    args = parse_args(
        argparse.ArgumentParser(description="Arguments for Starting the scheduler")
    )
