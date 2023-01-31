import asyncio
import os
import bisect
import math

import aiozmq
import uvloop
import zmq

from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.serialization import Serializer, cloudpickle_deserialization
from minio import Minio
from miniopy_async import Minio as Minio_async

from coordinator import Coordinator

SERVER_PORT = WORKER_PORT = 8888

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
SNAPSHOT_BUCKET_NAME: str = "universalis-snapshots"


class CoordinatorService:

    def __init__(self):
        self.networking = NetworkingManager()
        self.coordinator = Coordinator(WORKER_PORT)
        # background task references
        self.background_tasks = set()
        self.worker_ips: dict[int, str] = {}
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        self.minio_client_async: Minio_async = Minio_async(
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
                        channel_name = f'{operator_one}_{operator_two}_{int(partition_one) * len(self.partitions_to_ids[operator_two].keys()) + int(partition_two)}'
                        self.messages_received_intervals[str(self.partitions_to_ids[operator_two][partition_two])][channel_name] = [(0,0)]
        logging.warning(self.partitions_to_ids)

    def create_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
    
    async def find_recovery_line(self):
        marked_nodes = set()

        root_set_changed = True

        while(root_set_changed):
            root_set_changed = False
            # First iterate to create a set of all reachable nodes in the graph from the root set (mark all reachable nodes)
            for worker_id in self.recovery_graph_root_set.keys():
                snapshot_timestamp = self.recovery_graph_root_set[worker_id]
                marked_nodes = marked_nodes.union(await self.find_reachable_nodes((worker_id, snapshot_timestamp)))

            # If node in the root set is marked, replace it with an earlier checkpoint
            for worker_id in self.recovery_graph_root_set.keys():
                snapshot_timestamp = self.recovery_graph_root_set[worker_id]
                if (worker_id, snapshot_timestamp) in marked_nodes:
                    index = self.snapshot_timestamps[worker_id].index(snapshot_timestamp) - 1
                    if index < 0:
                        logging.error("Even the first snapshot got marked, this should not be possible. Index smaller then zero.")
                    else:
                        # Replace it with the checkpoint before the marked one and repeat the process.
                        self.recovery_graph_root_set[worker_id] = self.snapshot_timestamps[index]
                        root_set_changed = True


    # Recursive method to find all reachable nodes and add them to a set
    async def find_reachable_nodes(self, node):
        if len(self.recovery_graph[node]) == 0:
            return set(node)
        else:
            reachable_set = self.recovery_graph[node]
            for next_node in self.recovery_graph[node]:
                reachable_set = reachable_set.union(await self.find_reachable_nodes(next_node))
            return reachable_set
    
    async def send_restore_message(self):
        # First build a dict that contains for every worker the snapshot timestamp + offsets to replay based on the recovery line
        to_recover = {}
        for worker_id in self.recovery_graph_root_set.keys():
            # Initiate a corresponding tuple for every worker_id, tuple contains (snapshot_timestamp, {channel: offset})
            to_recover[worker_id] = (self.recovery_graph_root_set[worker_id], {})
        for worker_id in self.recovery_graph_root_set.keys():
            to_replay_for_snapshot = self.messages_to_replay[worker_id][self.recovery_graph_root_set[worker_id]]
            for channel in to_replay_for_snapshot.keys():
                # Split the channel string for its components (example: map_filter_27)
                snt_op, rec_op, channel_no = channel.split('_')
                # Divide the channel number by the amount of partitions of the receiving operator (in the example case: 27 / no_partitions(filter) )
                snt_op_partition = math.floor(int(channel_no) / len(self.partitions_to_ids[rec_op].keys()))
                # We now have the partition of the sending operator (example: map), so we can look up which worker_id hosts this partition
                worker_id_to_replay = self.partitions_to_ids[snt_op][str(snt_op_partition)]
                # The hosting worker_id should replay the channel in question from the last offset received for this checkpoint
                # In case of our example: the worker that hosts the calculated map partition should replay messages sent on channel map_filter_27 from corresponding offset.
                to_recover[str(worker_id_to_replay)][1][channel] = to_replay_for_snapshot[channel]
        for worker_id in to_recover.keys():
            # Now we have to send a message to every worker containing the tuple we just created.
            # The worker can then restore the snapshot with the corresponding timestamp and replay its outgoing channels from the given offsets.
            await self.request_recovery_from_checkpoint(worker_id, to_recover[worker_id])

    async def test_snapshot_recovery(self):
        await self.add_edges_between_workers()
        logging.warning(self.recovery_graph)
        await self.find_recovery_line()
        logging.warning(f"recovery line should be: {self.recovery_graph_root_set}")
        await self.send_restore_message()
        await self.clear_checkpoint_details()

    async def clear_checkpoint_details(self):
        # Removes all the checkpoint information that is no longer necessary after the recovery line has been found
        root_set = self.recovery_graph_root_set
        new_recovery_graph = {}
        new_messages_to_replay = {}
        new_messages_received_intervals = {}
        new_messages_sent_intervals = {}
        for worker_id in root_set.keys():
            # Build new recovery graph containing only root set
            new_recovery_graph[(worker_id, root_set[worker_id])] = set()
            # Set for every worker_id a new list of timestamps containing only the root set timestamp
            self.snapshot_timestamps[worker_id] = [root_set[worker_id]]
            # Keep only the messages to replay for the root set
            new_messages_to_replay[worker_id] = {root_set[worker_id]: self.messages_to_replay[worker_id][root_set[worker_id]]}
            # Remove all unnecessary info from the received and sent intervals
            new_messages_received_intervals[worker_id] = {}
            new_messages_sent_intervals[worker_id] = {}
            for channel in self.messages_received_intervals[worker_id].keys():
                new_messages_received_intervals[worker_id][channel] = []
                for offset_and_timestamp in self.messages_received_intervals[worker_id][channel]:
                    if offset_and_timestamp[1] == root_set[worker_id]:
                        new_messages_received_intervals[worker_id][channel].append(offset_and_timestamp)
            for channel in self.messages_sent_intervals[worker_id].keys():
                new_messages_sent_intervals[worker_id][channel] = []
                for offset_and_timestamp in self.messages_sent_intervals[worker_id][channel]:
                    if offset_and_timestamp[1] == root_set[worker_id]:
                        new_messages_sent_intervals[worker_id][channel].append(offset_and_timestamp)
        # Set the new recovery graph and messages to replay
        self.recovery_graph = new_recovery_graph
        self.messages_to_replay = new_messages_to_replay
        self.messages_received_intervals = new_messages_received_intervals
        self.messages_sent_intervals = new_messages_sent_intervals

    async def request_recovery_from_checkpoint(self, worker_id, restore_point):
        await self.networking.send_message(
            self.worker_ips[worker_id], WORKER_PORT,
            {
                "__COM_TYPE__": 'RECOVER_FROM_SNAPSHOT',
                "__MSG__": restore_point
            },
            Serializer.MSGPACK
        )

    async def store_messages(self, msg):
        # Decided to map the interval logs per workerid, since we are looking for edges between workers.
        _, worker_id, checkpoint_timestamp = msg['snapshot_name'].replace('.bin', '').split('_')
        messages_received = msg['last_messages_processed']
        messages_sent = msg['last_messages_sent']
        self.messages_to_replay[worker_id][int(checkpoint_timestamp)] = messages_received
        logging.warning(messages_received)
        # We create an ordered list based on (offset, snapshot_timestamp) tuples, such that we have an ordered interval list
        # This enables us to easily check between which two snapshot timestamps a message was sent/received, so that we can add the edges to the right nodes
        for message in messages_received.keys():
            if message not in self.messages_received_intervals[worker_id].keys():
                self.messages_received_intervals[worker_id][message] = []
            bisect.insort(self.messages_received_intervals[worker_id][message], (messages_received[message], int(checkpoint_timestamp)))
        for message in messages_sent.keys():
            if message not in self.messages_sent_intervals[worker_id].keys():
                self.messages_sent_intervals[worker_id][message] = []
            bisect.insort(self.messages_sent_intervals[worker_id][message], (messages_sent[message], int(checkpoint_timestamp)))

    async def add_edges_between_workers(self):
        for worker_id_rec in self.messages_received_intervals.keys():
            for worker_id_snt in self.messages_sent_intervals.keys():
                if worker_id_rec==worker_id_snt:
                    continue
                for channel in self.messages_received_intervals[worker_id_rec].keys():
                    if channel in self.messages_sent_intervals[worker_id_snt].keys():
                        rec_intervals = self.messages_received_intervals[worker_id_rec][channel]
                        snt_intervals = self.messages_sent_intervals[worker_id_snt][channel]
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
                            
                            # If there is overlap in the intervals an edge should be created from the node at the beginning of the sent interval
                            # To the node at the end of the received interval.
                            self.recovery_graph[(worker_id_snt, snt_intervals[snt_index-1][1])].add((worker_id_rec, rec_intervals[rec_index][1]))

                            # Increase the lowest interval end
                            if rec_interval_end < snt_interval_end:
                                rec_interval_start = rec_interval_end
                                rec_index += 1
                            elif rec_interval_end == snt_interval_end:
                                rec_interval_start = rec_interval_end
                                snt_interval_start = snt_interval_end
                                rec_index += 1
                                snt_index += 1
                            else:
                                snt_interval_start = snt_interval_end
                                snt_index += 1
                        
                        # Now arrived at the last node, meaning that if the received is bigger than the sent, there are orphan messages.
                        # This means that an edge should be added from the last sent to the last received.
                        if rec_intervals[len(rec_intervals) - 1][0] > snt_intervals[len(snt_intervals) - 1][0]:
                            self.recovery_graph[(worker_id_snt, snt_intervals[len(snt_intervals) - 1][1])].add((worker_id_rec, rec_intervals[len(rec_intervals) - 1][1]))

    # Processes the information sent from the worker about a snapshot.
    async def process_snapshot_information(self, message):
        snapshot_name = message['snapshot_name'].replace('.bin', '').split('_')
        # Store the snapshot timestamp in a sorted list
        bisect.insort(self.snapshot_timestamps[snapshot_name[1]], int(snapshot_name[2]))
        # Store the sent and received message information
        # This will be used later to add the edges between workers
        await self.store_messages(message)
        # Update root set
        if self.recovery_graph_root_set[snapshot_name[1]] < int(snapshot_name[2]):
            self.recovery_graph_root_set[snapshot_name[1]] = int(snapshot_name[2])
        # Add nodes and edges within a process to the recovery graph
        await self.add_to_recovery_graph(snapshot_name)
        
    async def add_to_recovery_graph(self, snapshot_name):
        # Recovery graph looks like the following:
        # The nodes (keys) are the (worker_id, snapshot_time)
        # The outgoing edges (values) are lists (sets) containing (worker_id, snapshot_time) nodes
        self.recovery_graph[(snapshot_name[1], int(snapshot_name[2]))] = set()
        snapshot_number = self.snapshot_timestamps[snapshot_name[1]].index(int(snapshot_name[2]))
        if snapshot_number > 0:
            # Look up the previous snapshot timestamp and add an edge from that snapshot to the one we are currently processing.
            self.recovery_graph[(snapshot_name[1], self.snapshot_timestamps[snapshot_name[1]][snapshot_number-1])].add((snapshot_name[1], snapshot_name[2]))

    def init_snapshot_minio_bucket(self):
        if not self.minio_client.bucket_exists(SNAPSHOT_BUCKET_NAME):
            self.minio_client.make_bucket(SNAPSHOT_BUCKET_NAME)

    async def main(self):
        router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")  # coordinator
        logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
        # ADD DIFFERENT LOGIC FOR TESTING STATE RECOVERY
        # No while loop, but for example a simple sleep and recover message
        # self.create_task(self.test_snapshot_recovery())
        while True:
            resp_adr, data = await router.read()
            deserialized_data: dict = self.networking.decode_message(data)
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                message_type: str = deserialized_data['__COM_TYPE__']
                message = deserialized_data['__MSG__']
                match message_type:
                    case 'SEND_EXECUTION_GRAPH':
                        # Received execution graph from a universalis client
                        self.create_task(self.schedule_operators(message))
                        logging.info(f"Submitted Stateflow Graph to Workers")
                    case 'REGISTER_WORKER':
                        # A worker registered to the coordinator
                        assigned_id = self.coordinator.register_worker(message)
                        self.worker_ips[str(assigned_id)] = message
                        reply = self.networking.encode_message(assigned_id, Serializer.MSGPACK)
                        router.write((resp_adr, reply))
                        self.recovery_graph_root_set[str(assigned_id)] = 0
                        self.snapshot_timestamps[str(assigned_id)] = [0]
                        self.recovery_graph[(str(assigned_id), 0)] = set()
                        self.messages_received_intervals[str(assigned_id)] = {}
                        self.messages_sent_intervals[str(assigned_id)] = {}
                        self.messages_to_replay[str(assigned_id)] = {}
                        logging.warning(f"Worker registered {message} with id {reply}")
                    case 'SNAPSHOT_TAKEN':
                        await self.process_snapshot_information(message)
                    case 'WORKER_FAILED':
                        logging.warning('Worker failed! Find recovery line.')
                        await self.test_snapshot_recovery()
                    case _:
                        # Any other message type
                        logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")


if __name__ == "__main__":
    uvloop.install()
    coordinator_service = CoordinatorService()
    asyncio.run(coordinator_service.main())
