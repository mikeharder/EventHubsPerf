using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Metadata;
using CommandLine;
using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime;
using System.Threading.Tasks;

namespace EventHubsConsumePerf
{
    class Program
    {
        private const string _eventHubName = "test";

        // Settings copied from https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-faq#how-much-does-a-single-capacity-unit-let-me-achieve
        private const int _messagesPerBatch = 100;
        private const int _bytesPerMessage = 1024;
        private static readonly byte[] _payload = new byte[_bytesPerMessage];

        public class Options
        {
            [Option('c', "clients", Default = 1)]
            public int Clients { get; set; }

            [Option('p', "partitions", Default = 1)]
            public int Partitions { get; set; }

            [Option('v', "verbose", Default = false)]
            public bool Verbose { get; set; }
        }

        static async Task Main(string[] args)
        {
            if (!GCSettings.IsServerGC)
            {
                throw new InvalidOperationException("Requires server GC");
            }

#if DEBUG
            throw new InvalidOperationException("Requires release configuration");
#endif

            var connectionString = Environment.GetEnvironmentVariable("EVENT_HUBS_CONNECTION_STRING");

            await Parser.Default.ParseArguments<Options>(args).MapResult(
                async o => await Run(connectionString, o.Partitions, o.Clients, o.Verbose),
                errors => Task.CompletedTask);
        }

        static async Task Run(string connectionString, int partitions, int clients, bool verbose)
        {
            await SendMessages(connectionString);
            // await ReceiveMessages(connectionString, partitions, clients, verbose);
        }

        static async Task ReceiveMessages(string connectionString, int numPartitions, int numClients, bool verbose)
        {
            Console.WriteLine($"Receiving messages from {numPartitions} partitions using {numClients} client instances");

            var clients = new EventHubClient[numClients];
            for (var i = 0; i < numClients; i++)
            {
                clients[i] = new EventHubClient(connectionString, _eventHubName);
            }

            try
            {
                var client = clients.First();
                var partitionIds = (await client.GetPartitionIdsAsync()).Take(numPartitions);
                var partitions = await Task.WhenAll(partitionIds.Select(id => client.GetPartitionPropertiesAsync(id)));

                var totalCount = (long)0;
                foreach (var partition in partitions)
                {
                    var begin = partition.BeginningSequenceNumber;
                    var end = partition.LastEnqueuedSequenceNumber;
                    var count = end - begin + 1;
                    totalCount += count;

                    if (verbose)
                    {
                        Console.WriteLine($"Partition: {partition.Id}, Begin: {begin}, End: {end}, Count: {count}");
                    }
                }
                if (verbose)
                {
                    Console.WriteLine($"Total Count: {totalCount}");
                }

                var consumers = new EventHubConsumer[numPartitions];
                for (var i = 0; i < numPartitions; i++)
                {
                    consumers[i] = clients[i % numClients].CreateConsumer(EventHubConsumer.DefaultConsumerGroupName, partitions[i].Id, EventPosition.Earliest);
                }

                try
                {
                    var receiveTasks = new Task<(int messagesReceived, long lastSequenceNumber)>[numPartitions];
                    var sw = Stopwatch.StartNew();
                    for (var i = 0; i < numPartitions; i++)
                    {
                        receiveTasks[i] = ReceiveAllMessages(consumers[i], partitions[i]);
                    }
                    var results = await Task.WhenAll(receiveTasks);
                    sw.Stop();

                    var elapsed = sw.Elapsed.TotalSeconds;
                    var messagesReceived = results.Select(r => r.messagesReceived).Sum();
                    var messagesPerSecond = messagesReceived / elapsed;
                    var megabytesPerSecond = (messagesPerSecond * _bytesPerMessage) / (1024 * 1024);

                    Console.WriteLine($"Received {messagesReceived} messages of size {_bytesPerMessage} in {elapsed:N2}s " +
                        $"({messagesPerSecond:N2} msg/s, {megabytesPerSecond:N2} MB/s)");
                }
                finally
                {
                    foreach (var consumer in consumers)
                    {
                        await consumer.DisposeAsync();
                    }
                }
            }
            finally
            {
                foreach (var client in clients)
                {
                    await client.DisposeAsync();
                }
            }
        }

        static async Task<(int messagesReceived, long lastSequenceNumber)> ReceiveAllMessages(EventHubConsumer consumer, PartitionProperties partition)
        {
            var messagesReceived = 0;
            var lastSequenceNumber = (long)-1;
            while (lastSequenceNumber < partition.LastEnqueuedSequenceNumber)
            {
                var events = await consumer.ReceiveAsync(_messagesPerBatch);
                messagesReceived += events.Count();
                lastSequenceNumber = events.Last().SequenceNumber;
            }

            return (messagesReceived, lastSequenceNumber);
        }

        static async Task SendMessages(string connectionString)
        {
            await using (var client = new EventHubClient(connectionString, _eventHubName))
            {
                await using (var producer = client.CreateProducer())
                {
                    for (var j = 0; j < 10000; j++)
                    {
                        Console.WriteLine(j);

                        var eventBatch = new EventData[_messagesPerBatch];
                        for (var i = 0; i < _messagesPerBatch; i++)
                        {
                            eventBatch[i] = new EventData(_payload);
                        }

                        await producer.SendAsync(eventBatch);
                    }
                }
            }
        }

    }
}
