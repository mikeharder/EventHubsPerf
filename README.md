# EventHubsPerf

## Create Azure Resources
1. Create an Event Hubs Namespace
   * Pricing Tier: Basic
   * Throughput Units: 20
2. Create an Event Hub
   * Name: `test`
3. Create a VM
   * Region: Same as Event Hubs Namespace
   * Image: Ubuntu Server 18.04 LTS
   * Size: DS3_v2 (4 cores, 14 GB RAM)
   * Inbound Port Rules: Allow SSH
   * OS Disk Type: Standard HDD
4. SSH to VM
5. Install Docker
   * https://get.docker.com

## Build and Run Benchmarks
1. SSH to VM
2. `export "EVENT_HUBS_CONNECTION_STRING=<your-connection-string>"`
3. `cd` to desired benchmark directory
4. Run `./build.sh` to create docker image
5. Run `./run.sh --help` to view command-line parameters
6. Run `./run.sh -p 1 -c 1` (with desired parameters) to run test
