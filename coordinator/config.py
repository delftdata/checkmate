import argparse


# Method to parse command-line arguments.
def config():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-cp", "--checkpointing_protocol", help="Choose the checkpointing protocol to use.",
                                  choices=["COR", "UNC", "CIC", "NOC"],
                                  nargs=1,
                                  default="NOC",
                                  action="store")
    arg_parser.add_argument("-ci", "--checkpointing_interval",
                            help="Choose a checkpointing interval.",
                            default=5,
                            action="store")
    arguments = arg_parser.parse_args()

    return arguments
