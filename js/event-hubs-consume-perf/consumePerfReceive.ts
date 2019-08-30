import { EventHubClient, EventPosition, PartitionProperties, EventHubConsumer } from '@azure/event-hubs';

async function main(): Promise<void> {
  // Wait for parameters from parent process
  const { messagesPerBatch, connectionString, eventHubName, numClients, firstPartition, numPartitions, verbose, debug }:
    {
      messagesPerBatch: number, connectionString: string, eventHubName: string,
      numClients: number, firstPartition: number, numPartitions: number, verbose: boolean, debug: boolean
    }
    = await new Promise<any>(resolve => {
      process.once('message', message => resolve(message));
    });

  if (debug) {
    console.log(`numClients: ${numClients}, firstPartition: ${firstPartition}, numPartitions: ${numPartitions}`);
  }

  // Create clients and consumers
  const clients: EventHubClient[] = [];
  for (let i = 0; i < numClients; i++) {
    clients[i] = new EventHubClient(connectionString, eventHubName);
  }

  try {
    const client = clients[0];

    const partitionIds = (await client.getPartitionIds()).slice(firstPartition, firstPartition + numPartitions);

    const partitions: PartitionProperties[] = [];
    for (let partitionId of partitionIds) {
      partitions.push(await client.getPartitionProperties(partitionId));
    }

    const consumers: EventHubConsumer[] = [];
    for (let i = 0; i < numPartitions; i++) {
      consumers[i] = clients[i % numClients].createConsumer(EventHubClient.defaultConsumerGroupName, partitionIds[i], EventPosition.earliest());
    }

    try {
      // Tell parent we are ready to receive
      process.send!("ready");

      // Wait for signal from parent process to start receiving
      await new Promise<any>(resolve => {
        process.once('message', message => resolve(message));
      });

      if (debug) {
        console.log(`starting receive on partitions ${firstPartition} to ${firstPartition + numPartitions - 1}`);
      }

      const receivePromises: Promise<number>[] = [];
      for (let i = 0; i < numPartitions; i++) {
        receivePromises[i] = receiveAllMessages(messagesPerBatch, consumers[i], partitions[i], debug);
      }
      const results = await Promise.all(receivePromises);

      // Send results to parent
      process.send!(results);
    }
    finally {
      for (let consumer of consumers) {
        await consumer.close();
      }
    }
  }
  finally {
    for (let client of clients) {
      await client.close();
    }
  }
}

async function receiveAllMessages(messagesPerBatch: number, consumer: EventHubConsumer, partition: PartitionProperties, debug: boolean): Promise<number> {
  let messagesReceived = 0;
  let lastSequenceNumber = -1;

  while (lastSequenceNumber < partition.lastEnqueuedSequenceNumber) {
    const events = await consumer.receiveBatch(messagesPerBatch);
    messagesReceived += events.length;
    lastSequenceNumber = events[events.length - 1].sequenceNumber;

    if (debug) {
      console.log(messagesReceived);
    }
  }

  return messagesReceived;
}

main().catch(err => {
  console.log('Error occurred: ', err);
});