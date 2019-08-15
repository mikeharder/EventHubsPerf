package com.microsoft.eventhubsconsumeperf;

import org.apache.commons.cli.*;

/**
 * Hello world!
 *
 */
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
    }
}
