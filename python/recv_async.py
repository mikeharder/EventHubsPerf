import argparse
import asyncio
import os
import time

from azure.eventhub.aio import EventHubClient
from azure.eventhub import EventPosition

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--clients", help="Number of client instances", default=1, type=int)
parser.add_argument("-p", "--partitions", help="Number of partitions to receive from", default=1, type=int)
parser.add_argument("-v", "--verbose", help="Enable verbose output", action="store_true")

args = parser.parse_args()

EVENT_HUB_NAME = "test"

MESSAGES_PER_BATCH = 100
BYTES_PER_MESSAGE = 1024

CONNECTION_STRING = os.environ.get("EVENT_HUBS_CONNECTION_STRING")

async def run(args):
    clients = []
    for i in range(args.clients):
        clients.append(EventHubClient.from_connection_string(CONNECTION_STRING, event_hub_path=EVENT_HUB_NAME))
    client = clients[0]
    
    partitionIds = (await client.get_partition_ids())[0:args.partitions]

    partitions = await asyncio.gather(*[client.get_partition_properties(p) for p in partitionIds])

    totalCount = 0
    for partition in partitions:
        begin = partition["beginning_sequence_number"]
        end = partition["last_enqueued_sequence_number"]
        count = end - begin + 1
        totalCount += count

        if args.verbose:
            print(f"Partition: {partition['id']}, Begin: {begin}, End: {end}, Count: {count}")

    if args.verbose:
        print(f"Total Count: {totalCount}")

    consumers = []
    for i in range(args.partitions):
        consumers.append(clients[i % args.clients].create_consumer(consumer_group="$default", partition_id=partitions[i]["id"], event_position=EventPosition("-1")))

    start = time.time()
    results = await asyncio.gather(*[receive_all_messages(consumers[i], partitions[i]) for i in range(args.partitions)])
    end = time.time()

    elapsed = end - start
    messagesReceived = sum(results)
    messagesPerSecond = messagesReceived / elapsed
    megabytesPerSecond = (messagesPerSecond * BYTES_PER_MESSAGE) / (1024 * 1024)

    print(f"Received {messagesReceived} messages of size {BYTES_PER_MESSAGE} in {elapsed}s ({messagesPerSecond} msg/s, {megabytesPerSecond} MB/s)")

async def receive_all_messages(consumer, partition):
    messagesReceived = 0
    lastSequenceNumber = -1
    while lastSequenceNumber < partition["last_enqueued_sequence_number"]:
        # BUG: Last batch call never returns if batch is unfilled and timeout is not specified
        events = await consumer.receive(max_batch_size=MESSAGES_PER_BATCH, timeout=1)
        
        messagesReceived += len(events)
        lastSequenceNumber = events[-1].sequence_number
        print("[" + partition["id"] + "] messagesReceived: " + str(messagesReceived))
        print("[" + partition["id"] + "] lastSequenceNumber: " + str(lastSequenceNumber))
    return messagesReceived

asyncio.run(run(args))
