import { EventHubClient } from '@azure/event-hubs';

const eventHubName = 'test';

async function main(): Promise<void> {
  const connectionString = process.env.EVENT_HUBS_CONNECTION_STRING;
  if (!connectionString) {
    throw 'Env var EVENT_HUBS_CONNECTION_STRING must be set';
  }

  const client = new EventHubClient(connectionString, eventHubName);
  try {
    const partitionIds = await client.getPartitionIds();
    console.log(partitionIds);

    const partitionsSequential = [
      await client.getPartitionProperties(partitionIds[0]),
      await client.getPartitionProperties(partitionIds[1])
    ];
    console.log(partitionsSequential);

    let partitionsParallel = await Promise.all([
      client.getPartitionProperties(partitionIds[2]),
      client.getPartitionProperties(partitionIds[3]),
    ]);
    console.log(partitionsParallel);
  }
  finally {
    client.close();
  }
}

main().catch(err => {
  console.log('Error occurred: ', err);
});
