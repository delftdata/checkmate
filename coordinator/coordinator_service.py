import asyncio
import os
import bisect
import math
import time

import aiozmq
import uvloop
import zmq

from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.serialization import Serializer
from minio import Minio

from coordinator import Coordinator
from config import config

SERVER_PORT = WORKER_PORT = 8888

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
SNAPSHOT_BUCKET_NAME: str = "universalis-snapshots"


class CoordinatorService(object):

    def __init__(self, checkpointing_protocol: str, checkpointing_interval: int):

        self.networking = NetworkingManager()
        self.router = None
        self.coordinator = Coordinator(WORKER_PORT)
        # background task references
        self.background_tasks = set()
        self.worker_ips: dict[int, str] = {}
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        self.partitions_to_ids = {}
        self.init_snapshot_minio_bucket()
        self.current_snapshots = {}
        self.recovery_graph_root_set = {}
        self.messages_received_intervals = {}
        self.messages_sent_intervals = {}
        self.recovery_graph = {}
        self.snapshot_timestamps = {}
        self.messages_to_replay = {}

        self.can_checkpoint = asyncio.Event()
        self.can_checkpoint.set()
        self.checkpointing_in_progress = asyncio.Event()
        self.checkpointing_in_progress.clear()

        # checkpointing config
        self.checkpointing_protocol: str = checkpointing_protocol
        self.checkpointing_interval: int = checkpointing_interval
        self.networking.set_checkpoint_protocol(checkpointing_protocol)
        logging.warning(f"Checkpointing protocol set: {checkpointing_protocol}")

        # Coordinated approach
        self.checkpoint_round = -1
        self.started_processing = {}
        self.channel_list = None
        # Send out checkpointing, only when all workers are done, increase round and wait for next checkpointing.
        self.done_checkpointing = {}
        self.last_confirmed_checkpoint_round = -1

        # Benchmarking metrics
        self.useless_checkpoints = 0

        self.failure_time = 0
        self.amount_of_failures = 0
        self.total_recovery_time = 0
        self.recovery_done = {}

        # Checkpointing time can either be a summation of all rounds (cor)
        # or summation of all individual checkpoint times (unc, cic)
        self.total_checkpointing_time = 0
        self.cor_start_time = 0
        self.amount_of_checkpoints = 0
        self.total_checkpoints_numbers = 0

        self.puller_task = ...

    async def schedule_operators(self, message):
        # Store return value (operators/partitions per workerid)
        self.partitions_to_ids = await self.coordinator.submit_stateflow_graph(self.networking, message)
        # For every channel, initialize a last_received to zero, to make sure checkpoints are replayed from offset 0.
        for operator_one in self.partitions_to_ids.keys():
            for operator_two in self.partitions_to_ids.keys():
                if operator_one == operator_two:
                    continue
                for partition_one in self.partitions_to_ids[operator_one].keys():
                    for partition_two in self.partitions_to_ids[operator_two].keys():
                        channel_name = (f'{operator_one}_{operator_two}_'
                                        f'{int(partition_one) * len(self.partitions_to_ids[operator_two].keys()) + int(partition_two)}')
                        self.messages_received_intervals[str(self.partitions_to_ids[operator_two][partition_two])][
                            operator_two][channel_name] = [(0, 0)]

    def create_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    def find_recovery_line(self):
        marked_nodes = set()

        root_set_changed = True

        while root_set_changed:
            root_set_changed = False
            # First iterate to create a set of all reachable nodes
            # in the graph from the root set (mark all reachable nodes)
            for worker_id in self.recovery_graph_root_set.keys():
                for op_name in self.recovery_graph_root_set[worker_id].keys():
                    snapshot_timestamp = int(self.recovery_graph_root_set[worker_id][op_name])
                    marked_nodes = marked_nodes.union(self.find_reachable_nodes((worker_id,
                                                                                 op_name,
                                                                                 snapshot_timestamp),
                                                                                len(self.recovery_graph.keys())))

            # If node in the root set is marked, replace it with an earlier checkpoint
            for worker_id in self.recovery_graph_root_set.keys():
                for op_name in self.recovery_graph_root_set[worker_id].keys():
                    snapshot_timestamp = self.recovery_graph_root_set[worker_id][op_name]
                    if (worker_id, op_name, snapshot_timestamp) in marked_nodes:
                        index = self.snapshot_timestamps[worker_id][op_name].index(snapshot_timestamp) - 1
                        if index < 0:
                            logging.error("Even the first snapshot got marked, this should not be possible."
                                          " Index smaller then zero.")
                        else:
                            self.useless_checkpoints += 1
                            # Replace it with the checkpoint before the marked one and repeat the process.
                            self.recovery_graph_root_set[worker_id][op_name] = int(
                                self.snapshot_timestamps[worker_id][op_name][index])
                            root_set_changed = True

    # Recursive method to find all reachable nodes and add them to a set
    def find_reachable_nodes(self, node, max_steps):
        if max_steps == 0:
            return set()
        if node not in self.recovery_graph.keys():
            logging.warning(f'Node was not found in: {self.recovery_graph}')
        reachable_set = self.recovery_graph[node]
        for next_node in self.recovery_graph[node]:
            reachable_set = reachable_set.union(self.find_reachable_nodes(next_node, max_steps - 1))
        return reachable_set

    async def send_restore_message(self):
        to_replay = self.find_channels_to_replay()

        for worker_id in to_replay.keys():
            # Now we have to send a message to every worker containing the tuple we just created.
            # The worker can then restore the snapshot with the corresponding
            # timestamp and replay its outgoing channels from the given offsets.
            await self.request_recovery_from_checkpoint(worker_id, to_replay[worker_id])

    def find_channels_to_replay(self):
        # Build a dict that contains for every worker the snapshot
        # timestamp + offsets to replay based on the recovery line
        to_replay = {}
        for worker_id in self.recovery_graph_root_set.keys():
            to_replay[worker_id] = {}
            for op_name in self.recovery_graph_root_set[worker_id].keys():
                # Initiate a corresponding tuple for every worker_id,
                # op_name. Tuple contains (snapshot_timestamp, {channel: offset})
                to_replay[worker_id][op_name] = (int(self.recovery_graph_root_set[worker_id][op_name]), {})
        for worker_id in self.recovery_graph_root_set.keys():
            for op_name in self.recovery_graph_root_set[worker_id].keys():
                if int(self.recovery_graph_root_set[worker_id][op_name]) == 0:
                    continue
                to_replay_for_snapshot = self.messages_to_replay[worker_id][op_name][int(
                    self.recovery_graph_root_set[worker_id][op_name])]
                for channel in to_replay_for_snapshot.keys():
                    # Split the channel string for its components (example: map_filter_27)
                    snt_op, rec_op, channel_no = channel.split('_')
                    # Divide the channel number by the amount of partitions of the receiving operator
                    # (in the example case: 27 / no_partitions(filter) )
                    snt_op_partition = math.floor(int(channel_no) / len(self.partitions_to_ids[rec_op].keys()))
                    # We now have the partition of the sending operator (example: map),
                    # so we can look up which worker_id hosts this partition
                    worker_id_to_replay = self.partitions_to_ids[snt_op][str(snt_op_partition)]
                    # The hosting worker_id should replay the channel in question
                    # from the last offset received for this checkpoint
                    # In case of our example: the worker that hosts the calculated map partition should
                    # replay messages sent on channel map_filter_27 from corresponding offset.
                    to_replay[str(worker_id_to_replay)][snt_op][1][channel] = to_replay_for_snapshot[channel]
        return to_replay

    async def test_snapshot_recovery(self):
        # TODO: Update methods one by one to incorporate operator.
        self.add_edges_between_workers()
        self.find_recovery_line()
        logging.warning(f"Time to find recovery line: {(time.time_ns() // 1000000) - self.failure_time}")
        await self.send_restore_message()
        self.clear_checkpoint_details()

    def clear_checkpoint_details(self):
        # Removes all the checkpoint information that is no longer necessary after the recovery line has been found
        root_set = self.recovery_graph_root_set
        new_recovery_graph = {}
        new_messages_to_replay = {}
        new_messages_received_intervals = {}
        new_messages_sent_intervals = {}
        for worker_id in root_set.keys():
            new_messages_to_replay[worker_id] = {}
            new_messages_received_intervals[worker_id] = {}
            new_messages_sent_intervals[worker_id] = {}
            for op_name in root_set[worker_id].keys():
                # Build new recovery graph containing only root set
                new_recovery_graph[(worker_id, op_name, root_set[worker_id][op_name])] = set()
                # Set for every worker_id a new list of timestamps containing only the root set timestamp
                self.snapshot_timestamps[worker_id][op_name] = [root_set[worker_id][op_name]]
                # Keep only the messages to replay for the root set
                if root_set[worker_id][op_name] == 0:
                    continue
                new_messages_to_replay[worker_id][op_name] = \
                    {root_set[worker_id][op_name]: self.messages_to_replay[worker_id][op_name][
                                                                            root_set[worker_id][op_name]]}
                # Remove all unnecessary info from the received and sent intervals
                new_messages_received_intervals[worker_id][op_name] = {}
                new_messages_sent_intervals[worker_id][op_name] = {}
                for channel in self.messages_received_intervals[worker_id][op_name].keys():
                    new_messages_received_intervals[worker_id][op_name][channel] = []
                    for offset_and_timestamp in self.messages_received_intervals[worker_id][op_name][channel]:
                        if offset_and_timestamp[1] == root_set[worker_id][op_name]:
                            new_messages_received_intervals[worker_id][op_name][channel].append(offset_and_timestamp)
                for channel in self.messages_sent_intervals[worker_id][op_name].keys():
                    new_messages_sent_intervals[worker_id][op_name][channel] = []
                    for offset_and_timestamp in self.messages_sent_intervals[worker_id][op_name][channel]:
                        if offset_and_timestamp[1] == root_set[worker_id][op_name]:
                            new_messages_sent_intervals[worker_id][op_name][channel].append(offset_and_timestamp)
        # Set the new recovery graph and messages to replay
        self.recovery_graph = new_recovery_graph
        self.messages_to_replay = new_messages_to_replay
        self.messages_received_intervals = new_messages_received_intervals
        self.messages_sent_intervals = new_messages_sent_intervals

    async def request_recovery_from_checkpoint(self, worker_id, restore_point):
        logging.warning(f'Sending restore point to worker: {worker_id} -> {restore_point}')
        await self.networking.send_message(
            self.worker_ips[worker_id], WORKER_PORT,
            {
                "__COM_TYPE__": 'RECOVER_FROM_SNAPSHOT',
                "__MSG__": restore_point
            },
            Serializer.MSGPACK
        )

    def store_messages(self, msg):
        # Decided to map the interval logs per workerid, since we are looking for edges between workers.
        _, worker_id, op_name, checkpoint_timestamp = msg['snapshot_name'].replace('.bin', '').split('_')
        messages_received = msg['last_messages_processed']
        messages_sent = msg['last_messages_sent']
        self.messages_to_replay[worker_id][op_name][int(checkpoint_timestamp)] = messages_received
        # We create an ordered list based on (offset, snapshot_timestamp) tuples,
        # such that we have an ordered interval list
        # This enables us to easily check between which two snapshot timestamps a message was sent/received,
        # so that we can add the edges to the right nodes
        for message in messages_received.keys():
            if message not in self.messages_received_intervals[worker_id][op_name].keys():
                self.messages_received_intervals[worker_id][op_name][message] = []
            bisect.insort(self.messages_received_intervals[worker_id][op_name][message],
                          (messages_received[message],
                           int(checkpoint_timestamp)))
        for message in messages_sent.keys():
            if message not in self.messages_sent_intervals[worker_id][op_name].keys():
                self.messages_sent_intervals[worker_id][op_name][message] = []
            bisect.insort(self.messages_sent_intervals[worker_id][op_name][message],
                          (messages_sent[message],
                           int(checkpoint_timestamp)))

    def add_edges_between_workers(self):
        for worker_id_rec in self.messages_received_intervals.keys():
            for worker_id_snt in self.messages_sent_intervals.keys():
                if worker_id_rec == worker_id_snt:
                    continue
                for rec_op in self.messages_received_intervals[worker_id_rec].keys():
                    for snt_op in self.messages_sent_intervals[worker_id_snt].keys():
                        for channel in self.messages_received_intervals[worker_id_rec][rec_op].keys():
                            if channel in self.messages_sent_intervals[worker_id_snt][snt_op].keys():
                                rec_intervals = self.messages_received_intervals[worker_id_rec][rec_op][channel]
                                snt_intervals = self.messages_sent_intervals[worker_id_snt][snt_op][channel]
                                rec_index = 1
                                snt_index = 1
                                rec_interval_start = rec_intervals[0][0]
                                snt_interval_start = snt_intervals[0][0]
                                while rec_index < len(rec_intervals) and snt_index < len(snt_intervals):
                                    rec_interval_end = rec_intervals[rec_index][0]
                                    snt_interval_end = snt_intervals[snt_index][0]

                                    # If there is no overlap, increase the lower interval and continue.
                                    if rec_interval_end < snt_interval_start:
                                        rec_interval_start = rec_interval_end
                                        rec_index += 1
                                        continue
                                    elif snt_interval_end < rec_interval_start:
                                        snt_interval_start = snt_interval_end
                                        snt_index += 1
                                        continue
                                    # If there is overlap in the intervals an edge should be created from
                                    # the node at the beginning of the sent interval
                                    # To the node at the end of the received interval.
                                    self.recovery_graph[(worker_id_snt,
                                                         snt_op,
                                                         int(snt_intervals[snt_index-1][1]))].add((worker_id_rec,
                                                                                                   rec_op,
                                                                                                   int(rec_intervals[rec_index][1])))
                                    # Increase the lowest interval end
                                    if rec_interval_end == snt_interval_end:
                                        rec_interval_start = rec_interval_end
                                        snt_interval_start = snt_interval_end
                                        rec_index += 1
                                        snt_index += 1
                                    elif rec_interval_end < snt_interval_end:
                                        rec_interval_start = rec_interval_end
                                        rec_index += 1
                                    else:
                                        snt_interval_start = snt_interval_end
                                        snt_index += 1
                                # Now arrived at the last node, meaning that if the received is bigger than the sent,
                                # there are orphan messages.
                                # This means that an edge should be added from the last sent to the last received.
                                if rec_intervals[len(rec_intervals) - 1][0] > snt_intervals[len(snt_intervals) - 1][0]:
                                    self.recovery_graph[(worker_id_snt,
                                                         snt_op,
                                                         int(snt_intervals[len(snt_intervals) - 1][1]))].add((worker_id_rec,
                                                                                                              rec_op,
                                                                                                              int(rec_intervals[len(rec_intervals) - 1][1])))

    # Processes the information sent from the worker about a snapshot.
    def process_snapshot_information(self, message):
        self.amount_of_checkpoints += 1
        self.total_checkpointing_time += message['snapshot_duration']
        # logging.warning(message)
        snapshot_name = message['snapshot_name'].replace('.bin', '').split('_')
        # Store the snapshot timestamp in a sorted list
        bisect.insort(self.snapshot_timestamps[snapshot_name[1]][snapshot_name[2]], int(snapshot_name[3]))
        # Store the sent and received message information
        # This will be used later to add the edges between workers
        self.store_messages(message)
        # Update root set
        if self.recovery_graph_root_set[snapshot_name[1]][snapshot_name[2]] < int(snapshot_name[3]):
            self.recovery_graph_root_set[snapshot_name[1]][snapshot_name[2]] = int(snapshot_name[3])
        # Add nodes and edges within a process to the recovery graph
        self.add_to_recovery_graph(snapshot_name)
        # Seems to work? (fingers crossed, will keep the warnings here just in case)
        # logging.warning(f'root set looks like: {self.recovery_graph_root_set}')
        # logging.warning(f'recovery graph: {self.recovery_graph}')

    def add_to_recovery_graph(self, snapshot_name):
        # Recovery graph looks like the following:
        # The nodes (keys) are the (worker_id, snapshot_time)
        # The outgoing edges (values) are lists (sets) containing (worker_id, snapshot_time) nodes
        self.recovery_graph[(snapshot_name[1], snapshot_name[2], int(snapshot_name[3]))] = set()
        snapshot_number = self.snapshot_timestamps[snapshot_name[1]][snapshot_name[2]].index(int(snapshot_name[3]))
        if snapshot_number > 0:
            # Look up the previous snapshot timestamp and
            # add an edge from that snapshot to the one we are currently processing.
            self.recovery_graph[(snapshot_name[1],
                                 snapshot_name[2],
                                 int(self.snapshot_timestamps[snapshot_name[1]][snapshot_name[2]][snapshot_number-1]))].add((snapshot_name[1],
                                                                                                                             snapshot_name[2],
                                                                                                                             int(snapshot_name[3])))

    async def coordinated_checkpointing(self, checkpointing_interval):
        while True:
            await self.can_checkpoint.wait()
            await asyncio.sleep(checkpointing_interval)
            if self.checkpoint_round == self.last_confirmed_checkpoint_round:
                self.checkpoint_round = self.checkpoint_round + 1
                self.checkpointing_in_progress.set()
                await self.start_coordinated_checkpoint()

    async def start_coordinated_checkpoint(self):
        for worker_id in self.worker_ips.keys():
            await self.networking.send_message(
                self.worker_ips[worker_id], WORKER_PORT,
                {
                    "__COM_TYPE__": 'TAKE_COORDINATED_CHECKPOINT',
                    "__MSG__": self.checkpoint_round
                },
                Serializer.MSGPACK
            )
        self.cor_start_time = time.time_ns() // 1000000

    async def get_metrics(self):
        await asyncio.sleep(90)
        logging.warning('GETTING METRICS!')
        offsets_per_second = {}
        total_network_size = 0
        protocol_specific_size = 0
        for worker_id in self.worker_ips.keys():
            worker_offsets, worker_tns, worker_pss = await self.networking.send_message_request_response(
                self.worker_ips[worker_id], WORKER_PORT,
                {
                    "__COM_TYPE__": 'GET_METRICS',
                    "__MSG__": ''
                },
                Serializer.MSGPACK
            )
            for op in worker_offsets.keys():
                if op not in offsets_per_second.keys():
                    offsets_per_second[op] = {}
                for part in worker_offsets[op].keys():
                    offsets_per_second[op][part] = worker_offsets[op][part]
            total_network_size += worker_tns
            protocol_specific_size += worker_pss
        total_network_size += self.networking.get_total_network_size()
        protocol_specific_size += self.networking.get_protocol_network_size()
        logging.warning(f'Amount of useless checkpoints: {self.useless_checkpoints}')
        if self.amount_of_failures > 0:
            logging.warning(f'Average recovery time: {self.total_recovery_time / self.amount_of_failures}')
        avg_cp_time = 0
        match self.checkpointing_protocol:
            case 'COR':
                if not self.amount_of_failures:
                    self.total_checkpoints_numbers = self.checkpoint_round
                if self.checkpoint_round > 0:
                    avg_cp_time = self.total_checkpointing_time / self.checkpoint_round
            case _:
                if not self.amount_of_failures:
                    self.total_checkpoints_numbers = self.amount_of_checkpoints
                if self.amount_of_checkpoints:
                    avg_cp_time = self.total_checkpointing_time / self.amount_of_checkpoints
        logging.warning(f'Average checkpointing time: {avg_cp_time}')
        logging.warning(f'Total network size: {total_network_size}, protocol size: {protocol_specific_size}')
        logging.warning(f'offsets per second: {offsets_per_second}')
        with open("metrics.txt", "w") as fp:
            fp.write(f'Average checkpointing time: {avg_cp_time}\n')
            fp.write(f'Total network size: {total_network_size}, protocol size: {protocol_specific_size}\n')
            fp.write(f'offsets per second: {offsets_per_second}\n')
            fp.write(f'Amount of useless checkpoints: {self.useless_checkpoints}\n')
            if self.amount_of_failures:
                fp.write(f'Average recovery time: {self.total_recovery_time / self.amount_of_failures}\n')
            else:
                fp.write('Average recovery time: 0\n')
            fp.write(f'Total number of checkpoints taken: {self.total_checkpoints_numbers}\n')

    def init_snapshot_minio_bucket(self):
        try:
            if not self.minio_client.bucket_exists(SNAPSHOT_BUCKET_NAME):
                self.minio_client.make_bucket(SNAPSHOT_BUCKET_NAME)
        except Exception:
            logging.warning("Unable to create minio bucket")

    async def coordinator_controller(self, deserialized_data, resp_adr):
        # ADD DIFFERENT LOGIC FOR TESTING STATE RECOVERY
        # No while loop, but for example a simple sleep and recover message
        # self.create_task(self.test_snapshot_recovery())
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        match message_type:
            case 'SEND_EXECUTION_GRAPH':
                # Received execution graph from a universalis client
                self.create_task(self.schedule_operators(message))
                for op in message.nodes.keys():
                    for id in self.worker_ips.keys():
                        self.recovery_graph_root_set[id][op] = 0
                        self.snapshot_timestamps[id][op] = [0]
                        self.messages_to_replay[id][op] = {0: {}}
                        self.recovery_graph[(id, op, 0)] = set()
                        self.messages_received_intervals[id][op] = {}
                        self.messages_sent_intervals[id][op] = {}
                        self.messages_to_replay[id][op] = {}
                logging.info("Submitted Stateflow Graph to Workers")
            case 'SEND_CHANNEL_LIST':
                self.channel_list = message
                for worker_id in self.worker_ips.keys():
                    await self.networking.send_message(
                        self.worker_ips[worker_id], WORKER_PORT,
                        {
                            "__COM_TYPE__": 'SEND_CHANNEL_LIST',
                            "__MSG__": message
                        },
                        Serializer.MSGPACK
                    )
            case 'REGISTER_WORKER':
                # A worker registered to the coordinator
                assigned_id = self.coordinator.register_worker(message)
                self.worker_ips[str(assigned_id)] = message
                reply = self.networking.encode_message(assigned_id, Serializer.MSGPACK)
                self.router.write((resp_adr, reply))
                self.recovery_graph_root_set[str(assigned_id)] = {}
                self.snapshot_timestamps[str(assigned_id)] = {}
                self.messages_received_intervals[str(assigned_id)] = {}
                self.messages_sent_intervals[str(assigned_id)] = {}
                self.messages_to_replay[str(assigned_id)] = {}
                self.started_processing[assigned_id] = False
                self.done_checkpointing[assigned_id] = False
                self.recovery_done[assigned_id] = False
                logging.info(f"Worker registered {message} with id {reply}")
                await self.networking.send_message(
                    message, WORKER_PORT,
                    {
                        "__COM_TYPE__": 'CHECKPOINT_PROTOCOL',
                        "__MSG__": (self.checkpointing_protocol, self.checkpointing_interval)
                    },
                    Serializer.MSGPACK
                )
            case 'STARTED_PROCESSING':
                self.started_processing[message] = True
                start_checkpointing = True
                for id in self.started_processing.keys():
                    start_checkpointing = start_checkpointing and self.started_processing[id]
                logging.warning(self.started_processing)
                if start_checkpointing:
                    logging.warning('All workers started processing.')
                    if self.checkpointing_protocol == 'COR':
                        logging.warning('Coordinated checkpointing started.')
                        self.create_task(self.coordinated_checkpointing(self.checkpointing_interval))
                    self.create_task(self.get_metrics())
            case 'RECOVERY_DONE':
                self.recovery_done[message] = True
                all_workers_done = True
                for id in self.recovery_done.keys():
                    all_workers_done = all_workers_done and self.recovery_done[id]
                if all_workers_done:
                    self.total_recovery_time += ((time.time_ns() // 1000000) - self.failure_time)
                    for id in self.recovery_done.keys():
                        self.recovery_done[id] = False
                    for id in self.done_checkpointing.keys():
                        self.done_checkpointing[id] = False
                    self.checkpointing_in_progress.clear()
                    self.can_checkpoint.set()
                    msg_tasks = []
                    for worker_id in self.worker_ips.keys():
                        msg_tasks.append(self.networking.send_message(
                            self.worker_ips[worker_id], WORKER_PORT,
                            {
                                "__COM_TYPE__": 'GLOBAL_RECOVERY_DONE',
                                "__MSG__": None
                            },
                            Serializer.MSGPACK
                        ))
                    await asyncio.gather(*msg_tasks)
            case 'COORDINATED_ROUND_DONE':
                self.done_checkpointing[message[0]] = True
                all_workers_done = True
                for id in self.done_checkpointing.keys():
                    all_workers_done = all_workers_done and self.done_checkpointing[id]
                if all_workers_done:
                    self.total_checkpointing_time += ((time.time_ns() // 1000000) - self.cor_start_time)
                    for id in self.done_checkpointing.keys():
                        self.done_checkpointing[id] = False
                    self.last_confirmed_checkpoint_round = message[1]
                    logging.warning(f'Checkpointing round {message[1]} done')
                    self.checkpointing_in_progress.clear()
            case 'SNAPSHOT_TAKEN':
                self.process_snapshot_information(message)
            case 'WORKER_FAILED':
                self.can_checkpoint.clear()
                # if we are in a checkpointing phase we need to reset everything.
                if self.checkpointing_in_progress.is_set():
                    self.checkpoint_round -= 1
                    for id in self.done_checkpointing.keys():
                        self.done_checkpointing[id] = False
                    self.checkpointing_in_progress.clear()
                self.failure_time = time.time_ns() // 1000000
                self.amount_of_failures += 1
                if self.checkpointing_protocol == 'COR':
                    for worker_id in self.worker_ips.keys():
                        await self.networking.send_message(
                            self.worker_ips[worker_id], WORKER_PORT,
                            {
                                "__COM_TYPE__": 'RECOVER_FROM_SNAPSHOT',
                                "__MSG__": self.last_confirmed_checkpoint_round
                            },
                            Serializer.MSGPACK
                        )
                    self.total_checkpoints_numbers = self.checkpoint_round
                else:
                    logging.warning('Worker failed! Find recovery line.')
                    if self.checkpointing_protocol in ('COR', 'UNC', 'CIC'):
                        await self.test_snapshot_recovery()
                        self.total_checkpoints_numbers = self.amount_of_checkpoints
            case _:
                # Any other message type
                logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")

    async def start_tcp_service(self):
        self.puller_task = asyncio.create_task(self.start_puller())
        self.router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{int(SERVER_PORT) + 1}")
        logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT} "
                     f"IP:{self.networking.host_name}")
        while True:
            # This is where we read from TCP, log at receiver
            resp_adr, data = await self.router.read()
            deserialized_data: dict = self.networking.decode_message(data)
            if '__COM_TYPE__' not in deserialized_data:
                logging.error("Deserialized data do not contain a message type")
            else:
                self.create_task(self.coordinator_controller(deserialized_data, resp_adr))

    async def start_puller(self):
        puller = await aiozmq.create_zmq_stream(zmq.PULL, bind=f"tcp://0.0.0.0:{SERVER_PORT}")
        while True:
            # This is where we read from TCP, log at receiver
            data = await puller.read()
            deserialized_data: dict = self.networking.decode_message(data[0])
            if '__COM_TYPE__' not in deserialized_data:
                logging.error("Deserialized data do not contain a message type")
            else:
                self.create_task(self.coordinator_controller(deserialized_data, None))

    async def main(self):
        await self.start_tcp_service()


if __name__ == "__main__":
    args = config()
    coordinator_service = CoordinatorService(args.checkpointing_protocol[0], int(args.checkpointing_interval))
    uvloop.run(coordinator_service.main())
