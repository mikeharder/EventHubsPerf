import { EventHubClient, EventPosition, PartitionProperties, EventHubConsumer } from '@azure/event-hubs';

async function main(): Promise<void> {
  // Wait for parameters from parent process
  const { messagesPerBatch, connectionString, eventHubName, numClients, firstPartition, numPartitions, verbose }:
    {
      messagesPerBatch: number, connectionString: string, eventHubName: string,
      numClients: number, firstPartition: number, numPartitions: number, verbose: boolean
    }
    = await new Promise<any>(resolve => {
      process.once('message', message => resolve(message));
    });

  if (verbose) {
    console.log(`numClients: ${numClients}, firstPartition: ${firstPartition}, numPartitions: ${numPartitions}`);
  }

  // Create clients and consumers
  const clients: EventHubClient[] = [];
  for (let i = 0; i < numClients; i++) {
    clients[i] = new EventHubClient(connectionString, eventHubName);
  }

  const client = clients[0];

  const partitionIds = (await client.getPartitionIds()).slice(firstPartition, numPartitions);

  const partitions: PartitionProperties[] = [];
  for (let partitionId of partitionIds) {
    partitions.push(await client.getPartitionProperties(partitionId));
  }

  const consumers: EventHubConsumer[] = [];
  for (let i = 0; i < numPartitions; i++) {
    consumers[i] = clients[i % numClients].createConsumer(EventHubClient.defaultConsumerGroupName, partitionIds[i], EventPosition.earliest());
  }

  // Wait for signal from parent process to start receiving
  await new Promise<any>(resolve => {
    process.once('message', message => resolve(message));
  });

  const receivePromises: Promise<number>[] = [];
  for (let i = 0; i < numPartitions; i++) {
    receivePromises[i] = receiveAllMessages(messagesPerBatch, consumers[i], partitions[i]);
  }
  const results = await Promise.all(receivePromises);

  // Send results to parent
  process.send!(results);
}

async function receiveAllMessages(messagesPerBatch: number, consumer: EventHubConsumer, partition: PartitionProperties): Promise<number> {
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