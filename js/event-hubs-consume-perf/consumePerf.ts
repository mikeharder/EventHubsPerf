import { EventHubClient, EventPosition, PartitionProperties, EventHubConsumer } from '@azure/event-hubs';
import { performance } from 'perf_hooks';
import commander from 'commander';
import { fork, ChildProcess } from 'child_process';

const eventHubName = 'test';

// Settings copied from https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-faq#how-much-does-a-single-capacity-unit-let-me-achieve
const messagesPerBatch = 100;
const bytesPerMessage = 1024;
const payload = Buffer.alloc(bytesPerMessage);

async function main(): Promise<void> {
  commander
    .option('-c, --clients <clients>', 'number of client instances', 1)
    .option('-p, --partitions <partitions>', 'number of partitions to receive from', 1)
    .option('-r, --processes <processes>', 'number of processes to use', 1)
    .option('-v, --verbose', 'verbose', false);

  commander.parse(process.argv);

  const connectionString = process.env.EVENT_HUBS_CONNECTION_STRING;
  if (!connectionString) {
    throw 'Env var EVENT_HUBS_CONNECTION_STRING must be set';
  }

  await run(connectionString, commander.partitions, commander.clients, commander.processes, commander.verbose);
}

async function run(connectionString: string, numPartitions: number, numClients: number, numProcesses: number, verbose: boolean): Promise<void> {
  await receiveMessages(connectionString, numPartitions, numClients, numProcesses, verbose);
}

async function receiveMessages(connectionString: string, numPartitions: number, numClients: number, numProcesses: number, verbose: boolean): Promise<void> {
  console.log(`Receiving messages from ${numPartitions} partition(s) using ${numClients} client instanc(es) and ${numProcesses} child process(es)`);

  const client = new EventHubClient(connectionString, eventHubName);

  try {
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

    // Create child processes
    const childProcesses: ChildProcess[] = [];

    for (let i = 0; i < numProcesses; i++) {
      const childProcess = fork('consumePerfReceive.js');

      const clientsPerProcess = numClients / numProcesses;
      const partitionsPerProcess = numPartitions / numProcesses;
      const firstPartition = i * partitionsPerProcess;

      // Send parameters
      childProcess.send({
        messagesPerBatch: messagesPerBatch, connectionString: connectionString, eventHubName: eventHubName,
        numClients: clientsPerProcess, firstPartition: firstPartition, numPartitions: numPartitions / numProcesses,
        verbose: verbose
      })
      childProcesses[i] = childProcess;
    }

    const receivePromises: Promise<number[]>[] = [];
    const startMs = performance.now();
    for (let i = 0; i < numProcesses; i++) {
      const childProcess = childProcesses[i];
      
      // Tell child process to start receiving
      receivePromises[i] = new Promise<number[]>(resolve => {
        childProcess.send("go");

        childProcess.once('message', (msg: number[]) => {
          resolve(msg);
        });
      });
    }
    const results = await Promise.all(receivePromises);
    const endMs = performance.now();

    const elapsedSeconds = (endMs - startMs) / 1000;
    const messagesReceived = results.reduce((a, b) => a + b.reduce((c, d) => c + d, 0), 0);
    const messagesPerSecond = messagesReceived / elapsedSeconds;
    const megabytesPerSecond = (messagesPerSecond * bytesPerMessage) / (1024 * 1024);

    console.log(`Received ${messagesReceived} messages of size ${bytesPerMessage} in ${elapsedSeconds.toFixed(2)}s ` +
      `(${messagesPerSecond.toFixed(2)} msg/s, ${megabytesPerSecond.toFixed(2)} MB/s)`);
  } finally {
    await client.close();
  }
}

main().catch(err => {
  console.log('Error occurred: ', err);
});
