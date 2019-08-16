import argparse
import asyncio
import os

from azure.eventhub.aio import EventHubClient

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--clients", help="Number of client instances", default=1, type=int)
parser.add_argument("-p", "--partitions", help="Number of partitions to receive from", default=1, type=int)
parser.add_argument("-v", "--verbose", help="Enable verbose output", action="store_true")

args = parser.parse_args()

EVENT_HUB_NAME = "test"

MESSAGES_PER_BATCH = 100
BYTES_PER_MESSAGE = 1024

CONNECTION_STRING = os.environ.get("EVENT_HUBS_CONNECTION_STRING")

clients = []
for i in range(args.clients):
    clients.append(EventHubClient.from_connection_string(CONNECTION_STRING, event_hub_path=EVENT_HUB_NAME))

client = clients[0]
partitionIds = asyncio.run(client.get_partition_ids())[0:args.partitions]

print(partitionIds)
