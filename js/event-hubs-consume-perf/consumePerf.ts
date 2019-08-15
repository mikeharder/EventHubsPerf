import { EventHubClient, EventPosition, PartitionProperties, EventHubConsumer } from '@azure/event-hubs';
import { performance } from 'perf_hooks';
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

    // Concurrent calls to getPartitionProperties() are extremely slow (Azure/azure-sdk-for-js#4767)
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

    const consumers: EventHubConsumer[] = [];
    for (let i = 0; i < numPartitions; i++) {
      consumers[i] = clients[i % numClients].createConsumer(EventHubClient.defaultConsumerGroupName, partitions[i].partitionId, EventPosition.earliest());
    }

    try {
      const receivePromises: Promise<number>[] = [];
      const startMs = performance.now();
      for (let i = 0; i < numPartitions; i++) {
        receivePromises[i] = receiveAllMessages(consumers[i], partitions[i]);
      }
      const results = await Promise.all(receivePromises);
      const endMs = performance.now();

      const elapsedSeconds = (endMs - startMs) / 1000;
      const messagesReceived = results.reduce((a, b) => a + b, 0);
      const messagesPerSecond = messagesReceived / elapsedSeconds;
      const megabytesPerSecond = (messagesPerSecond * bytesPerMessage) / (1024 * 1024);

      console.log(`Received ${messagesReceived} messages of size ${bytesPerMessage} in ${elapsedSeconds.toFixed(2)}s ` +
        `(${messagesPerSecond.toFixed(2)} msg/s, ${megabytesPerSecond.toFixed(2)} MB/s)`);
    }
    finally {
      for (let consumer of consumers) {
        await consumer.close();
      }
    }
  } finally {
    for (let client of clients) {
      await client.close();
    }
  }
}

async function receiveAllMessages(consumer: EventHubConsumer, partition: PartitionProperties): Promise<number> {
  let messagesReceived = 0;
  let lastSequenceNumber = -1;

  while (lastSequenceNumber < partition.lastEnqueuedSequenceNumber) {
    const events = await consumer.receiveBatch(messagesPerBatch);
    messagesReceived += events.length;
    lastSequenceNumber = events[events.length - 1].sequenceNumber;
  }

  return messagesReceived;
}

main().catch(err => {
  console.log('Error occurred: ', err);
});
