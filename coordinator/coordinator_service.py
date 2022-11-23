import asyncio
import os

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

    async def schedule_operators(self, message):
        await self.coordinator.submit_stateflow_graph(self.networking, message)

    def create_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def restore_from_failure(self):
        graph = await self.create_recovery_graph()

        # Placeholder graph:
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

        # Don't forget to purge messages and checkpoints that happened before root set/intervals.

        return await self.find_recovery_line(graph)

    async def create_recovery_graph(self):
        # Checkpoints as nodes
        # Connect all nodes in their order (C1,2 -> C1,3, etc.)
        # Connect nodes Cx,i with Cy,j if there is a message from Ix,i to Iy,j (interval comes after the checkpoint)
        # To be able to do the above we need to know from every message between workers the checkpoint before send and before receive
        return
    
    async def find_recovery_line(self, graph):
        # Create the root set; last checkpoint from every worker
        root_set = [4, 3]

        root_set_changed = True

        while(root_set_changed):
            root_set_changed = False
            for worker in range(len(root_set)):
                checkpoint = root_set[worker]
                edges = graph[(worker, checkpoint)]
                for edge in edges:
                    if edge[0] != worker and edge[1] < root_set[edge[0]]:
                        root_set[edge[0]] = edge[1]
                        root_set_changed = True

        return root_set

    def parse_snapshot_name(self, snapshot_name):
        elements = snapshot_name.strip(".bin").split("_")
        self.current_snapshots[elements[1]].append(elements[2])

    async def test_snapshot_recovery(self):
        await asyncio.sleep(40)
        list_of_snapshots = list(map(lambda x: x.object_name, self.minio_client.list_objects(SNAPSHOT_BUCKET_NAME)))
        for name in list_of_snapshots:
            self.parse_snapshot_name(name)
        root_set = await self.get_snapshots_root_set()
        logging.warning(f"Requesting recovery for root set: {root_set}")
        for key in root_set.keys():
            await self.request_recovery_from_checkpoint(key, root_set[key])
        
    async def get_snapshots_root_set(self):
        root_set = {}
        for key in self.current_snapshots.keys():
            root_set[key] = max(self.current_snapshots[key])
        return root_set

    async def request_recovery_from_checkpoint(self, worker_id, snapshot_timestamp):
        self.id = await self.networking.send_message(
            self.worker_id_to_host[worker_id], WORKER_PORT,
            {
                "__COM_TYPE__": 'RECOVER_FROM_SNAPSHOT',
                "__MSG__": f"snapshot_{worker_id}_{snapshot_timestamp}.bin"
            },
            Serializer.MSGPACK
        )

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
                        self.worker_ips[assigned_id] = message
                        reply = self.networking.encode_message(assigned_id, Serializer.MSGPACK)
                        router.write((resp_adr, reply))
                        self.current_snapshots[str(assigned_id)] = []
                        logging.info(f"Worker registered {message} with id {reply}")
                    case _:
                        # Any other message type
                        logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")


if __name__ == "__main__":
    uvloop.install()
    coordinator_service = CoordinatorService()
    asyncio.run(coordinator_service.main())
