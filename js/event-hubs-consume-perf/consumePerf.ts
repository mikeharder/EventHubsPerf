import { EventHubClient, EventPosition, PartitionProperties } from '@azure/event-hubs';
import commander from 'commander';

const eventHubName = 'test';

// Settings copied from https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-faq#how-much-does-a-single-capacity-unit-let-me-achieve
const messagesPerBatch = 100;
const bytesPerMessage = 1024;
const payload = Buffer.alloc(bytesPerMessage);

async function main(): Promise<void> {
  commander
    .option('-c, --clients <clients>', 'number of client instances', 1)
    .option('-p, --partitions <partitions>', 'number of partitions to receive from', 1)
    .option('-v, --verbose', 'verbose', false);

  commander.parse(process.argv);

  const connectionString = process.env.EVENT_HUBS_CONNECTION_STRING;
  if (!connectionString) {
    throw 'Env var EVENT_HUBS_CONNECTION_STRING must be set';
  }

  await run(connectionString, commander.partitions, commander.clients, commander.verbose);
}

async function run(connectionString: string, numPartitions: number, numClients: number, verbose: boolean): Promise<void> {
  await receiveMessages(connectionString, numPartitions, numClients, verbose);
}

async function receiveMessages(connectionString: string, numPartitions: number, numClients: number, verbose: boolean): Promise<void> {
  console.log(`Receiving messages from ${numPartitions} partitions using ${numClients} client instances`);

  const clients: EventHubClient[] = [];
  for (let i = 0; i < numClients; i++) {
    clients[i] = new EventHubClient(connectionString, eventHubName);
  }

  try {
    const client = clients[0];

    const partitionIds = (await client.getPartitionIds()).slice(0, numPartitions);

    const partitions: PartitionProperties[] = [];
    for (let partitionId of partitionIds) {
      partitions.push(await client.getPartitionProperties(partitionId));
    }

    // If multiple calls to getPartitionProperties() are made concurrently, the returned Promise never resolve
    // const partitions = await Promise.all(partitionIds.map(id => client.getPartitionProperties(id)));

    let totalCount = 0;
    for (let partition of partitions) {
      const begin = partition.beginningSequenceNumber;
      const end = partition.lastEnqueuedSequenceNumber;
      const count = end - begin + 1;
      totalCount += count;

      if (verbose) {
        console.log(`Partition: ${partition.partitionId}, Begin: ${begin}, End: ${end}, Count: ${count}`)
      }
    }
    if (verbose) {
      console.log(`Total Count: ${totalCount}`);
    }
  } finally {
    for (let client of clients) {
      await client.close();
    }
  }

  //       var consumers = new EventHubConsumer[numPartitions];
  //       for (var i = 0; i < numPartitions; i++)
  //       {
  //           consumers[i] = clients[i % numClients].CreateConsumer(EventHubConsumer.DefaultConsumerGroupName, partitions[i].Id, EventPosition.Earliest);
  //       }

  //       try
  //       {
  //           var receiveTasks = new Task<(int messagesReceived, long lastSequenceNumber)>[numPartitions];
  //           var sw = Stopwatch.StartNew();
  //           for (var i = 0; i < numPartitions; i++)
  //           {
  //               receiveTasks[i] = ReceiveAllMessages(consumers[i], partitions[i]);
  //           }
  //           var results = await Task.WhenAll(receiveTasks);
  //           sw.Stop();

  //           var elapsed = sw.Elapsed.TotalSeconds;
  //           var messagesReceived = results.Select(r => r.messagesReceived).Sum();
  //           var messagesPerSecond = messagesReceived / elapsed;
  //           var megabytesPerSecond = (messagesPerSecond * _bytesPerMessage) / (1024 * 1024);

  //           Console.WriteLine($"Received {messagesReceived} messages of size {_bytesPerMessage} in {elapsed:N2}s " +
  //               $"({messagesPerSecond:N2} msg/s, {megabytesPerSecond:N2} MB/s)");
  //       }
  //       finally
  //       {
  //           foreach (var consumer in consumers)
  //           {
  //               await consumer.DisposeAsync();
  //           }
  //       }
  //   }
}

main().catch(err => {
  console.log('Error occurred: ', err);
});
