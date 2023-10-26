import json
import sys


def main(file_name):
    """
    Get avg jct
    """
    with open(file_name, "r") as fin:
        data_job = json.load(fin)
    avg_jct = _get_avg_jct(data_job)
    print("Avg JCT {}".format(avg_jct))


def _get_avg_jct(time_dict):
    """
    Fetch the avg jct from the dict
    """
    values = list(time_dict.values())
    count = 0
    jct_time = 0
    for v in values:
        jct_time += v[1] - v[0]
        count += 1

    return jct_time / count


if __name__ == "__main__":
    main(sys.argv[1])
