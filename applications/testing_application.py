import sys
from blox_enumerator import bloxEnumerate


def main(args):
    run_times = 2
    enumerator = bloxEnumerate(range(2))
    for ictr, key in enumerator:
        print(ict, key)
        time.sleep(10)


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
