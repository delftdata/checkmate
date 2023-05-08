import asyncio

from universalis.common.operator import BaseOperator
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.logging import logging

from .base_scheduler import BaseScheduler


class RoundRobin(BaseScheduler):

    @staticmethod
    async def schedule(workers: dict[int, tuple[str, int]],
                       execution_graph: StateflowGraph,
                       network_manager):
        operator_partition_locations: dict[str, dict[str, tuple[str, int]]] = {}
        worker_locations = [StateflowWorker(worker[0], worker[1]) for worker in workers.values()]
        worker_assignments: dict[tuple[str, int], list[tuple[BaseOperator, int]]] = {(worker.host, worker.port): []
                                                                                     for worker in worker_locations}

        total_partitions_per_operator = {}

        for operator_name, operator in iter(execution_graph):
            total_partitions_per_operator[operator_name] = operator.n_partitions
            for partition in range(operator.n_partitions):
                current_worker = worker_locations.pop(0)
                worker_assignments[(current_worker.host, current_worker.port)].append((operator, partition))
                if operator_name in operator_partition_locations:
                    operator_partition_locations[operator_name].update({str(partition): (current_worker.host,
                                                                                         current_worker.port)})
                else:
                    operator_partition_locations[operator_name] = {str(partition): (current_worker.host,
                                                                                    current_worker.port)}
                worker_locations.append(current_worker)

        # We want to be able to map a location to a worker id, so we invert the 'workers' dictionary
        location_to_worker_id = {}
        for key in workers.keys():
            location_to_worker_id[workers[key]] = key

        # With this inverted dict, we can map (operator, partition) to a worker id.
        # This can be used in the coordinator service to map the message streams that have to be replayed to the corresponding sender id.
        # For example, if we have to replay filter_1_map_4 from offset x we can look up (filter, 1) to get the id of the worker that should replay from x.
        partitions_to_ids = {}
        for op in operator_partition_locations.keys():
            partitions_to_ids[op] = {}
            for part in operator_partition_locations[op].keys():
                partitions_to_ids[op][part] = location_to_worker_id[operator_partition_locations[op][part]]

        tasks = [
            asyncio.ensure_future(
                network_manager.send_message(worker[0], worker[1],
                                             {"__COM_TYPE__": 'RECEIVE_EXE_PLN',
                                              "__MSG__": (operator_partitions,
                                                          operator_partition_locations,
                                                          workers,
                                                          execution_graph.operator_state_backend,
                                                          total_partitions_per_operator)}))
            for worker, operator_partitions in worker_assignments.items()]

        await asyncio.gather(*tasks)

        return partitions_to_ids

        # Should return the operators/partitions per workerid
