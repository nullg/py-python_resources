#!/usr/bin/python3

import sys


def place_filter(country_name=["Australia"]):
    """ This mapper selects location which is within the provided country name
    Input format: place_id \t woeid \t latitude \t longitude \t place_name \t place_type_id \t place_url
    Output format: place_id \t place_url
    """
    for line in sys.stdin:
        # Clean input and split it
        parts = line.strip().split("\t")

        # Check that the line is of the correct format
        if len(parts) != 7:
            continue

        place_id, place_url = parts[0].strip(), parts[6].strip()

        # Extract the country from the url
        country = place_url.strip('/').split("/")[0]

        # Check that the country is one of the provided country name
        if country in country_name:
            print(place_id + "\t" + place_url)

if __name__ == "__main__":
    python_arguments = sys.argv

    country_name = ["Australia"]
    if len(python_arguments) > 1:
        country_name = python_arguments[1:]

    place_filter(country_name)