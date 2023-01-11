import asyncio
import os
import bisect

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
        self.init_snapshot_minio_bucket()
        self.current_snapshots = {}
        self.recovery_graph_root_set = {}
        self.messages_received_intervals = {}
        self.messages_sent_intervals = {}
        self.recovery_graph = {}
        self.snapshot_timestamps = {}

    async def schedule_operators(self, message):
        # Store return value (operators/partitions per workerid)
        partitions_to_ids = await self.coordinator.submit_stateflow_graph(self.networking, message)
        logging.warning(partitions_to_ids)

    def create_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def restore_from_failure(self):
        graph = await self.create_recovery_graph()

        # Placeholder graph:
        # Graph should look like the following: key = (workerid, snapshot_timestamp),
        # Value is another dict (or tuple) with the outgoing edges (workerid, snapshot_timestamp)
        # And the offsets which to replay from for each channel if that snapshot is restored (last messages processed before snapshot).
        graph = {
            (0,0): [(0,1), (1,0)],
            (0,1): [(0,2), (1,0)],
            (0,2): [(0,3), (1,1)],
            (0,3): [(0,4), (1,2)],
            (0,4): [(1,3)],
            (1,0): [(1,1), (0,1)],
            (1,1): [(1,2), (0,2)],
            (1,2): [(1,3)],
            (1,3): [(0,3)],

        }

        # Don't forget to remove the checkpoints that are taken before the recovery line (purge)
        # Also, after the recovery line is found, make sure to check from which offset each worker has to replay messages.

        return await self.find_recovery_line(graph)
    
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

    async def test_snapshot_recovery(self):
        await asyncio.sleep(40)
        await self.add_edges_between_workers()
        logging.warning(self.recovery_graph)
        await self.find_recovery_line()
        logging.warning(f"recovery line should be: {self.recovery_graph_root_set}")

    async def request_recovery_from_checkpoint(self, worker_id, snapshot_timestamp):
        # Needs to be updated to take into account the offsets to replay.
        await self.networking.send_message(
            self.worker_ips[worker_id], WORKER_PORT,
            {
                "__COM_TYPE__": 'RECOVER_FROM_SNAPSHOT',
                "__MSG__": f"snapshot_{worker_id}_{snapshot_timestamp}.bin"
            },
            Serializer.MSGPACK
        )

    async def store_messages(self, msg):
        # Decided to map the interval logs per workerid, since we are looking for edges between workers.
        _, worker_id, checkpoint_timestamp = msg['snapshot_name'].replace('.bin', '').split('_')
        messages_received = msg['last_messages_processed']
        messages_sent = msg['last_messages_sent']
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
        self.create_task(self.test_snapshot_recovery())
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
                        self.snapshot_timestamps[str(assigned_id)] = []
                        self.messages_received_intervals[str(assigned_id)] = {}
                        self.messages_sent_intervals[str(assigned_id)] = {}
                        logging.warning(f"Worker registered {message} with id {reply}")
                    case 'SNAPSHOT_TAKEN':
                        await self.process_snapshot_information(message)
                    case _:
                        # Any other message type
                        logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")


if __name__ == "__main__":
    uvloop.install()
    coordinator_service = CoordinatorService()
    asyncio.run(coordinator_service.main())
