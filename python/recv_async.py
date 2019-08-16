import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--clients", help="Number of client instances", default=1)
parser.add_argument("-p", "--partitons", help="Number of partitions to receive from", default=1)
parser.add_argument("-v", "--verbose", help="Enable verbose output", action="store_true")

args = parser.parse_args()

