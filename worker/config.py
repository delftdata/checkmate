import argparse

from universalis.common.logging import logging


# Method to parse command-line arguments.
def config():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-f", "--failure",
                            help="Boolean flag to set or not a worker failure",
                            default=False,
                            action="store_true")
    arguments = arg_parser.parse_known_args()

    return arguments[0]
