#!/usr/bin/python3

import sys


def replicate_join():
    """ This mapper selects photos on the filtered location and outputs the photo id, date taken and place url.
    Input format: photo_id \t owner \t tags \t date_taken \t place_id \t accuracy
    Output format: photo_id \t date_take \t place_url
    """
    place_id_to_url = {}

    # From the distributed cache, open the list of filtered place and Map place_id -> place_url
    with open("part-00000") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 2:
                continue
            place_id, place_url = parts[0].strip(), parts[1].strip()

            place_id_to_url[place_id] = place_url

    for line in sys.stdin:
        # Clean input and split it
        line = line.strip()
        parts = line.split("\t")

        # Check that the line is of the correct format
        if len(parts) != 6:
            continue

        photo_id, date_taken, place_id = parts[0].strip(), parts[3].strip(), parts[4].strip()

        # Check that the place_id of the photo is in the filtered location
        if place_id in place_id_to_url:
            # Get the place url given the place id
            place_url = place_id_to_url[place_id]

            # Output photo_id, date_taken and place url
            print("{}\t{}\t{}" .format(photo_id, date_taken, place_url))

if __name__ == "__main__":
    replicate_join()