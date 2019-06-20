#!/usr/bin/python3

import sys


def read_map_output(file):
    """ Return an iterator for key, value pair extracted from file (sys.stdin)
    Input format:  key \t value
    Output format: (key, value)
    """
    for line in file:
        yield line.strip().split("\t", 1)


def tag_reducer():
    """ This reducer reads in tag, owner-counts key-value pairs and returns the
    total sum of owner-counts per tags in the same format as the input to allow
    the reducer to be used as a combiner.
    Input format: tag \t {owner=count}
    Output format: tag \t {owner=count}
    """
    current_tag = ""
    owner_count = {}

    data = read_map_output(sys.stdin)
    for tag, ownercounts in data:
        if current_tag != tag:

            if current_tag != "":
                output = current_tag + "\t"
                for owner, count in owner_count.items():
                    output += "{}={}, ".format(owner, count)
                print(output.strip())

            current_tag = tag
            owner_count = {}

        for ownercount in ownercounts.strip(",").split(","):
            owner, count = ownercount.strip().split("=")
            owner_count[owner] = owner_count.get(owner, 0) + int(count)

    if current_tag != "":
        output = current_tag + "\t"
        for owner, count in owner_count.items():
            output += "{}={}, ".format(owner, count)
        print(output.strip())

if __name__ == "__main__":
    tag_reducer()
