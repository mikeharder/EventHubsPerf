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

    // Takes about 100 ms
    console.log(`${new Date().toISOString()} Before sequential`);
    const partitionsSequential = [
      await client.getPartitionProperties(partitionIds[0]),
      await client.getPartitionProperties(partitionIds[1])
    ];
    console.log(`${new Date().toISOString()} After sequential`);
    console.log(partitionsSequential);

    // Takes exactly 90 seconds, which seems too close to a round number to be a coincidence.  Maybe corresponds to an internal timeout?
    console.log(`${new Date().toISOString()} Before parallel`);
    const partitionsParallel = await Promise.all([
      client.getPartitionProperties(partitionIds[2]),
      client.getPartitionProperties(partitionIds[3]),
    ]);
    console.log(`${new Date().toISOString()} After paralllel`);
    console.log(partitionsParallel);
  }
  finally {
    client.close();
  }
}

main().catch(err => {
  console.log('Error occurred: ', err);
});
