import asyncio

import aiozmq
import uvloop
import zmq

from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.serialization import Serializer

from coordinator import Coordinator

SERVER_PORT = WORKER_PORT = 8888


class CoordinatorService:

    def __init__(self):
        self.networking = NetworkingManager()
        self.coordinator = Coordinator(WORKER_PORT)
        # background task references
        self.background_tasks = set()
        self.worker_ips: dict[int, str] = {}

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

    async def main(self):
        router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")  # coordinator
        logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
        root_set = await self.restore_from_failure()
        logging.warning(f"Calculated root set to be: {root_set}")
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
                        logging.info(f"Worker registered {message} with id {reply}")
                    case _:
                        # Any other message type
                        logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")


if __name__ == "__main__":
    uvloop.install()
    coordinator_service = CoordinatorService()
    asyncio.run(coordinator_service.main())
