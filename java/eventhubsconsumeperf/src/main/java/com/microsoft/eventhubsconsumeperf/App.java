package com.microsoft.eventhubsconsumeperf;

import java.util.List;

import com.azure.messaging.eventhubs.EventHubAsyncClient;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.PartitionProperties;

import org.apache.commons.cli.*;

import reactor.core.publisher.Flux;

public class App {
    private static final String _eventHubName = "test";

    // Settings copied from
    // https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-faq#how-much-does-a-single-capacity-unit-let-me-achieve
    private static final int _messagesPerBatch = 100;
    private static final int _bytesPerMessage = 1024;
    private static final byte[] _payload = new byte[_bytesPerMessage];

    public static void main(String[] args) {
        Options options = new Options();

        Option clientsOption = new Option("c", "clients", true, "Number of client instances");
        options.addOption(clientsOption);

        Option partitionsOption = new Option("p", "partitions", true, "Number of partitions");
        options.addOption(partitionsOption);

        Option verboseOption = new Option("v", "verbose", false, "Enables verbose output");
        options.addOption(verboseOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("eventhubsconsumeperf", options);
            System.exit(1);
        }

        int clients = Integer.parseInt(cmd.getOptionValue("clients", "1"));
        int partitions = Integer.parseInt(cmd.getOptionValue("partitions", "1"));
        boolean verbose = cmd.hasOption("verbose");

        String connectionString = System.getenv("EVENT_HUBS_CONNECTION_STRING");
        if (connectionString == null || connectionString.isEmpty()) {
            System.out.println("Environment variable EVENT_HUBS_CONNECTION_STRING must be set");
            System.exit(1);
        }

        ReceiveMessages(connectionString, partitions, clients, verbose);
    }

    static void ReceiveMessages(String connectionString, int numPartitions, int numClients, boolean verbose) {
        System.out.println(String.format("Receiving messages from %d partitions using %d client instances",
                numPartitions, numClients));

        EventHubAsyncClient[] clients = new EventHubAsyncClient[numClients];
        for (int i = 0; i < numClients; i++) {
            clients[i] = new EventHubClientBuilder().connectionString(connectionString, _eventHubName)
                    .buildAsyncClient();
        }

        try {
            EventHubAsyncClient client = clients[0];

            Flux<String> partitionIds = client.getPartitionIds().take(numPartitions);
            List<PartitionProperties> partitions = partitionIds.flatMap(id -> client.getPartitionProperties(id))
                    .collectSortedList((PartitionProperties p1, PartitionProperties p2) -> Integer
                            .compare(Integer.parseInt(p1.id()), Integer.parseInt(p2.id())))
                    .block();

            long totalCount = 0;
            for (PartitionProperties partition : partitions) {
                long begin = partition.beginningSequenceNumber();
                long end = partition.lastEnqueuedSequenceNumber();
                long count = end - begin + 1;
                totalCount += count;

                if (verbose) {
                    System.out.println(String.format("Partition: %s, Begin: %d, End: %d, Count: %d", partition.id(),
                            begin, end, count));
                }
            }
            if (verbose) {
                System.out.println(String.format("Total Count: %d", totalCount));
            }
        } finally {
            for (EventHubAsyncClient client : clients) {
                client.close();
            }
        }

        // Workaround for "IllegalThreadStateException on shutdown from maven exec plugin"
        // https://github.com/ReactiveX/RxJava/issues/2833
        System.exit(0);
    }
}
