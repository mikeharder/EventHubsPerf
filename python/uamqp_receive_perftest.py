import time
import threading
import argparse
import logging
import asyncio
try:
    from urllib import quote_plus #Py2
except Exception:
    from urllib.parse import quote_plus

import uamqp
from uamqp import authentication

#logging.basicConfig(level=logging.INFO)

global_msg_cnt_dict = {}

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--partitions", help="Number of partitions to receive from", default=1, type=int)
parser.add_argument("-m", "--messages_cnt", help="Number of messages from", default=10000, type=int)
parser.add_argument("-l", "--link_credit", help="Link credit", default=3000, type=int)
parser.add_argument("-v", "--verbose", help="Enable verbose output", action="store_false")
parser.add_argument("-t", "--run_type", help="1 for sync receive with multiple threads,"
                                             " 2 for sync receive with single thread, 3 for async receive", default=3, type=int)
parser.add_argument("-w", "--wait_timeout", help="timeout when receive, unit is milliseconds", default=20, type=int)

args = parser.parse_args()

MESSAGES_PER_BATCH = args.link_credit
BYTES_PER_MESSAGE = 1024

CONNECTION_STRING = "your connection string"
live_eventhub_config = {}


def _parse_conn_str(conn_str):
    endpoint = None
    shared_access_key_name = None
    shared_access_key = None
    entity_path = None
    for element in conn_str.split(';'):
        key, _, value = element.partition('=')
        if key.lower() == 'endpoint':
            endpoint = value.rstrip('/')
        elif key.lower() == 'hostname':
            endpoint = value.rstrip('/')
        elif key.lower() == 'sharedaccesskeyname':
            shared_access_key_name = value
        elif key.lower() == 'sharedaccesskey':
            shared_access_key = value
        elif key.lower() == 'entitypath':
            entity_path = value
    if not all([endpoint, shared_access_key_name, shared_access_key]):
        raise ValueError("Invalid connection string")
    return endpoint, shared_access_key_name, shared_access_key, entity_path


def client_receive_sync(args, partition, clients_arr):
    receive_client = clients_arr[partition]
    messagesReceived = 0
    print('start receiving {}'.format(partition))
    start = time.time()
    while messagesReceived < args.messages_cnt:
        batch = receive_client.receive_message_batch(max_batch_size=MESSAGES_PER_BATCH, timeout=args.wait_timeout)  # timeout is milliseconds
        if len(batch) > 0:
            messagesReceived += len(batch)
        if args.verbose:
            print("Current iteration, Partition: {},  current received batch size: {}, current partition total received count: {}".format(
                    partition, len(batch), messagesReceived))

    elapsed = time.time() - start

    if args.verbose:
        print("Current iteration, Partition: {},  current received batch size: {}, current partition total received count: {}".format(
                partition, len(batch), messagesReceived))

    messagesPerSecond = messagesReceived / elapsed
    megabytesPerSecond = (messagesPerSecond * BYTES_PER_MESSAGE) / (1024 * 1024)

    global_msg_cnt_dict[partition] = messagesReceived

    print(
        f"Received {messagesReceived} messages of size {BYTES_PER_MESSAGE} in {elapsed}s ({messagesPerSecond} msg/s, {megabytesPerSecond} MB/s) from {partition}")
    return messagesReceived


async def client_receive_async(args, client, partition):
    messagesReceived = 0
    print('start receiving {}'.format(partition))
    start = time.time()
    while messagesReceived < args.messages_cnt:
        batch = await client.receive_message_batch_async(max_batch_size=MESSAGES_PER_BATCH, timeout=args.wait_timeout)  # timeout is milliseconds
        if len(batch) > 0:
            messagesReceived += len(batch)
        if args.verbose:
            print("Current iteration, Partition: {},  current received batch size: {}, current partition total received count: {}".format(
                    partition, len(batch), messagesReceived))
    elapsed = time.time() - start

    if args.verbose:
        print("Current iteration, Partition: {},  current received batch size: {}, current partition total received count: {}".format(
                partition, len(batch), messagesReceived))

    messagesPerSecond = messagesReceived / elapsed
    megabytesPerSecond = (messagesPerSecond * BYTES_PER_MESSAGE) / (1024 * 1024)

    global_msg_cnt_dict[partition] = messagesReceived

    print(
        f"Received {messagesReceived} messages of size {BYTES_PER_MESSAGE} in {elapsed}s ({messagesPerSecond} msg/s, {megabytesPerSecond} MB/s) from {partition}")
    return messagesReceived


def create_and_open_receive_client(args, partition, clients_arr=None):
    print("Creating and opening receive client:{}".format(partition))
    uri = "sb://{}/{}".format(live_eventhub_config['hostname'], live_eventhub_config['event_hub'])
    sas_auth = authentication.SASTokenAuth.from_shared_access_key(
        uri, live_eventhub_config['key_name'], live_eventhub_config['access_key'])

    source = "amqps://{}/{}/ConsumerGroups/{}/Partitions/{}".format(
        live_eventhub_config['hostname'],
        live_eventhub_config['event_hub'],
        '$default',
        str(partition))
    global_msg_cnt_dict[partition] = 0
    receive_client = uamqp.ReceiveClient(source, auth=sas_auth, debug=True, timeout=5000, prefetch=args.link_credit)
    receive_client.open()
    while not receive_client.client_ready():
        time.sleep(0.05)

    if clients_arr:
        clients_arr[partition] = receive_client

    print("Receive client:{} is ready to receive".format(partition))
    return receive_client


def sync_receive_with_multiple_threads(args):
    threads = []

    clients_arr = [None] * args.partitions

    for i in range(args.partitions):
        thread = threading.Thread(target=create_and_open_receive_client, args=(args, i, clients_arr))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print('All clients are ready to receive')

    threads = []

    start = time.time()
    for i in range(args.partitions):
        thread = threading.Thread(target=client_receive_sync, args=(args, i, clients_arr))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    elapsed = time.time() - start

    total_message = 0

    for value in global_msg_cnt_dict.values():
        total_message = total_message + value

    messagesPerSecond = total_message / elapsed
    print("The total speed is {} msg/s, received in {}".format(messagesPerSecond, elapsed))

    for client in clients_arr:
        client.close()


def sync_receive_with_just_one_thread(args):
    receive_clients = []
    for i in range(args.partitions):
        receive_clients.append(create_and_open_receive_client(args, str(i)))
        global_msg_cnt_dict[i] = 0

    print('All Receivers are opened and ready')
    stop_flag = False
    start = time.time()
    while not stop_flag:
        stop_flag = True
        for i in range(args.partitions):
            if global_msg_cnt_dict[i] < args.messages_cnt:
                stop_flag = False
                batch = receive_clients[i].receive_message_batch(max_batch_size=MESSAGES_PER_BATCH, timeout=args.wait_timeout)
                if len(batch) > 0:
                    global_msg_cnt_dict[i] += len(batch)
                if args.verbose:
                    print("Current iteration, Partition: {},  current received batch size: {}, current partition total received count: {}".format(i, len(batch), global_msg_cnt_dict[i]))

            if args.verbose:
                print("Current iteration, Partition: {},  current received batch size: {}, current partition total received count: {}".format(
                        i, len(batch), i))
    elapsed = time.time() - start

    total_message = 0
    for i in range(args.partitions):
        total_message += global_msg_cnt_dict[i]

    messagesPerSecond = total_message / elapsed
    print("The total speed is {} msg/s, received in {}".format(messagesPerSecond, elapsed))

    for i in range(args.partitions):
        receive_clients[i].close()


async def async_create_and_open_receive_client(args, partition):
    print('Creating and opening receiver:{}'.format(partition))
    uri = "sb://{}/{}".format(live_eventhub_config['hostname'], live_eventhub_config['event_hub'])
    sas_auth = authentication.SASTokenAsync.from_shared_access_key(
        uri, live_eventhub_config['key_name'], live_eventhub_config['access_key'])

    source = "amqps://{}/{}/ConsumerGroups/{}/Partitions/{}".format(
        live_eventhub_config['hostname'],
        live_eventhub_config['event_hub'],
        '$default',
        partition)

    receive_client = uamqp.ReceiveClientAsync(source, auth=sas_auth, debug=True, timeout=5000, prefetch=args.link_credit)
    await receive_client.open_async()
    while not await receive_client.client_ready_async():
        asyncio.sleep(0.05)
    print('Receiver:{} is ready to receive'.format(partition))
    return receive_client


async def async_receive_messages(args):
    receive_clients = await asyncio.gather(*[async_create_and_open_receive_client(args, str(i)) for i in range(args.partitions)])
    for i in range(args.partitions):
        global_msg_cnt_dict[i] = 0

    start = time.time()
    await asyncio.gather(
        *[client_receive_async(args, receive_clients[i], i) for i in range(args.partitions)])
    elapsed = time.time() - start

    total_message = 0
    for i in range(args.partitions):
        total_message += global_msg_cnt_dict[i]

    messagesPerSecond = total_message / elapsed
    print("The total speed is {} msg/s, received in {}".format(messagesPerSecond, elapsed))
    await asyncio.gather(*[client.close_async() for client in receive_clients])


if __name__ == '__main__':

    hostname, policy, key, entity = _parse_conn_str(CONNECTION_STRING)

    live_eventhub_config = {
        'hostname': hostname.split('//', 2)[-1],
        'event_hub': entity,
        'key_name': policy,
        'access_key': key,
    }

    if args.run_type == 1:
        print('Running sync receive with multiple threads')
        sync_receive_with_multiple_threads(args)
    elif args.run_type == 2:
        print('Running sync receive with single thread')
        sync_receive_with_just_one_thread(args)
    else:
        print('Running async receive')
        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_receive_messages(args))
