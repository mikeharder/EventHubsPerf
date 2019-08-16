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

    # var consumers = new EventHubConsumer[numPartitions];
    # for (var i = 0; i < numPartitions; i++)
    # {
    #     consumers[i] = clients[i % numClients].CreateConsumer(EventHubConsumer.DefaultConsumerGroupName, partitions[i].Id, EventPosition.Earliest);
    # }

    # try
    # {
    #     var receiveTasks = new Task<(int messagesReceived, long lastSequenceNumber)>[numPartitions];
    #     var sw = Stopwatch.StartNew();
    #     for (var i = 0; i < numPartitions; i++)
    #     {
    #         receiveTasks[i] = ReceiveAllMessages(consumers[i], partitions[i]);
    #     }
    #     var results = await Task.WhenAll(receiveTasks);
    #     sw.Stop();

    #     var elapsed = sw.Elapsed.TotalSeconds;
    #     var messagesReceived = results.Select(r => r.messagesReceived).Sum();
    #     var messagesPerSecond = messagesReceived / elapsed;
    #     var megabytesPerSecond = (messagesPerSecond * _bytesPerMessage) / (1024 * 1024);

    #     Console.WriteLine($"Received {messagesReceived} messages of size {_bytesPerMessage} in {elapsed:N2}s " +
    #         $"({messagesPerSecond:N2} msg/s, {megabytesPerSecond:N2} MB/s)");
    # }


asyncio.run(run(args))
