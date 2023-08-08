import asyncio
import io
import os
import sys
import time
import random
from timeit import default_timer as timer
from math import ceil
import concurrent.futures

import aiozmq
import uvloop
import zmq
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError
from minio import Minio

from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.operator import Operator
from universalis.common.serialization import Serializer, compressed_cloudpickle_deserialization, compressed_cloudpickle_serialization, msgpack_serialization
from universalis.common.kafka_producer_pool import KafkaProducerPool
from universalis.common.kafka_consumer_pool import KafkaConsumerPool

from worker.operator_state.in_memory_state import InMemoryOperatorState
from worker.operator_state.redis_state import RedisOperatorState
from worker.operator_state.stateless import Stateless
from worker.run_func_payload import RunFuncPayload
from worker.checkpointing.uncoordinated_checkpointing import UncoordinatedCheckpointing
from worker.checkpointing.cic_checkpointing import CICCheckpointing
from worker.checkpointing.coordinated_checkpointing import CoordinatedCheckpointing

SERVER_PORT: int = 8888
DISCOVERY_HOST: str = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT: int = int(os.environ['DISCOVERY_PORT'])
KAFKA_URL: str = os.getenv('KAFKA_URL', None)
INGRESS_TYPE = os.getenv('INGRESS_TYPE', None)
EGRESS_TOPIC_NAME: str = 'universalis-egress'

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
SNAPSHOT_BUCKET_NAME: str = "universalis-snapshots"


class Worker(object):

    def __init__(self):
        self.checkpoint_protocol = None
        self.checkpoint_interval = 5
        self.checkpointing = None

        self.channel_list = None

        self.id: int = -1
        self.networking = NetworkingManager()
        self.router = None
        self.kafka_egress_producer_pool = None
        self.kafka_ingress_consumer_pool = None
        self.operator_state_backend = None
        self.registered_operators: dict[tuple[str, int], Operator] = {}
        self.dns: dict[str, dict[str, tuple[str, int]]] = {}
        self.topic_partitions: list[TopicPartition] = []
        # worker_id: (host, port)
        self.peers: dict[int, tuple[str, int]] = {}
        self.local_state: InMemoryOperatorState | RedisOperatorState | Stateless = Stateless()
        # background task references
        self.background_tasks = set()
        self.total_partitions_per_operator = {}
        self.function_tasks = set()
        # snapshot
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        self.snapshot_state_lock: asyncio.Lock = asyncio.Lock()
        # snapshot event
        self.snapshot_event: asyncio.Event = asyncio.Event()
        self.snapshot_event.set()
        # CIC variables
        self.waiting_for_exe_graph = True
        # Coordinated variables
        self.notified_coordinator = False
        self.message_buffer = {}
        # failure
        self.no_failure_event = asyncio.Event()
        self.no_failure_event.set()
        # message locks
        self.last_message_processed_lock = asyncio.Lock()
        self.kafka_lock = asyncio.Lock()

        # checkpoint start event
        self.start_checkpointing = asyncio.Event()
        self.start_checkpointing.clear()

        self.offsets_per_second = {}
        self.kafka_consumer: AIOKafkaConsumer = ...
        self.channels = {}

        self.snapshot_taken_message_size = 0

    async def run_function(
            self,
            payload: RunFuncPayload,
            send_from=None
    ) -> bool:
        # logging.warning(f"run_function -> snapshot_event: {self.snapshot_event.is_set()}")
        if send_from is not None:
            if self.checkpoint_protocol == 'UNC' or self.checkpoint_protocol == 'CIC':
                # Necessary for uncoordinated checkpointing
                tmp_str = send_from['operator_partition'] * self.total_partitions_per_operator[payload.operator_name] \
                            + payload.partition
                incoming_channel = f"{send_from['operator_name']}_{payload.operator_name}_{tmp_str}"
                async with self.last_message_processed_lock:
                    await self.checkpointing.set_last_messages_processed(payload.operator_name,
                                                                        incoming_channel,
                                                                        send_from['kafka_offset'])
        success: bool = True
        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]
        response = await operator_partition.run_function(
            payload.key,
            payload.request_id,
            payload.timestamp,
            payload.function_name,
            payload.params
        )
    
        # If exception we need to add it to the application logic aborts
        if isinstance(response, Exception):
            success = False
        # If request response send the response
        if payload.response_socket is not None:
            self.router.write(
                (payload.response_socket, self.networking.encode_message(
                    response,
                    Serializer.MSGPACK
                ))
            )
        # If we have a response, and it's not part of the chain send it to kafka
        elif response is not None:
            # If Exception transform it to string for Kafka
            if isinstance(response, Exception):
                kafka_response = str(response)
            else:
                kafka_response = response
            # If fallback add it to the fallback replies else to the response buffer
            self.create_task(next(self.kafka_egress_producer_pool).send_and_wait(
                EGRESS_TOPIC_NAME,
                key=payload.request_id,
                value=msgpack_serialization(kafka_response),
                partition=self.id-1
            ))

        return success

    @staticmethod
    def async_snapshot(
            snapshot_name,
            snapshot_data,
            message_encoder,
            coordinator_info: dict | None = None):
        minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        # minio_client.trace_on(sys.stderr)
        bytes_file: bytes = compressed_cloudpickle_serialization(snapshot_data)
        # logging.warning(f'finished compression of {snapshot_name}')
        minio_client.put_object(bucket_name=SNAPSHOT_BUCKET_NAME,
                                object_name=snapshot_name,
                                data=io.BytesIO(bytes_file),
                                length=len(bytes_file))
        
        
        # logging.warning(f"{snapshot_name} send to minio.")
        if coordinator_info is not None:
            msg = message_encoder(
                {
                    "__COM_TYPE__": 'SNAPSHOT_TAKEN',
                    "__MSG__": coordinator_info
                },
                Serializer.MSGPACK)
            sync_socket_to_coordinator = zmq.Context().socket(zmq.DEALER)
            sync_socket_to_coordinator.connect(f'tcp://{DISCOVERY_HOST}:{DISCOVERY_PORT}')
            sync_socket_to_coordinator.send(msg)
            sync_socket_to_coordinator.close()
            # return msgpack_serialization({
            #         "__COM_TYPE__": 'SNAPSHOT_TAKEN',
            #         "__MSG__": coordinator_info
            #     }).__sizeof__()
        return 0

    # if you want to use this run it with self.create_task(self.take_snapshot())
    async def take_snapshot(self, pool: concurrent.futures.ProcessPoolExecutor, operator, cic_clock=0, cor_round=-1):
        if isinstance(self.local_state, InMemoryOperatorState):
            await self.no_failure_event.wait()
            snapshot_start = time.time_ns() // 1000000
            self.local_state: InMemoryOperatorState
            async with self.snapshot_state_lock:
                await asyncio.gather(*self.function_tasks)
                self.function_tasks = set()
                # Flush the current kafka message buffer from networking to make sure the messages are in Kafka.
                last_messages_sent = await self.networking.flush_kafka_producer_pool(operator)
                snapshot_data = {}
                match self.checkpoint_protocol:
                    case 'UNC':
                        snapshot_data = self.checkpointing.get_snapshot_data(operator, last_messages_sent)
                    case 'CIC':
                        snapshot_data = self.checkpointing.get_snapshot_data(operator, last_messages_sent)
                        snapshot_data['cic_clock'] = cic_clock
                    case 'COR':
                        snapshot_data = self.checkpointing.get_snapshot_data(operator)
                    case _:
                        logging.warning('Unknown protocol, no snapshot data added.')
                snapshot_data['local_state_data'] = self.local_state.data[operator]
                snapshot_time = cor_round
                if snapshot_time == -1:
                    snapshot_time = time.time_ns() // 1_000_000
                snapshot_name: str = f"snapshot_{self.id}_{operator}_{snapshot_time}.bin"
                coordinator_info = None
                if self.checkpoint_protocol == 'UNC' or self.checkpoint_protocol == 'CIC':
                    snapshot_duration = (time.time_ns() // 1000000) - snapshot_start
                    coordinator_info = {'last_messages_processed': snapshot_data['last_messages_processed'],
                                        'last_messages_sent': last_messages_sent,
                                        'snapshot_name': snapshot_name,
                                        'snapshot_duration': snapshot_duration}
                    # logging.warning(f"snapshot_name: {snapshot_name}, coordinator_info: {coordinator_info}, snapshot_data: {snapshot_data}")
                loop = asyncio.get_running_loop()
                loop.run_in_executor(pool, self.async_snapshot,
                                     snapshot_name, snapshot_data, self.networking.encode_message, coordinator_info)\
                                        .add_done_callback(self.update_network_sizes)
                snapshot_end = time.time_ns() // 1000000
                if snapshot_end - snapshot_start > 40:
                    logging.warning(f'Operator: {operator}, Total time: {snapshot_end - snapshot_start}')
        else:
            logging.warning("Snapshot currently supported only for in-memory operator state")

    def update_network_sizes(self, future):
        self.networking.total_network_size += future.result()
        if self.checkpoint_protocol == 'UNC':
            self.networking.additional_uncoordinated_size += future.result()
        elif self.checkpoint_protocol == 'CIC':
            self.networking.additional_cic_size += future.result()

    async def restore_from_snapshot(self, snapshot_to_restore, operator_name):
        state_to_restore = self.minio_client.get_object(
            bucket_name=SNAPSHOT_BUCKET_NAME,
            object_name=snapshot_to_restore
        ).data
        restoring_data = time.time_ns() // 1000000
        async with self.snapshot_state_lock:
            deserialized_data = compressed_cloudpickle_deserialization(state_to_restore)
            self.local_state.data[operator_name] = deserialized_data['local_state_data']
        logging.warning(f'restoring snapshot data took: {(time.time_ns() // 1000000) - restoring_data}')
        last_kafka_consumed = {}
        find_replays = time.time_ns() // 1000000
        if 'last_kafka_consumed' in deserialized_data.keys():
            last_kafka_consumed = deserialized_data['last_kafka_consumed']
        if self.checkpoint_protocol == 'COR':
            to_replay = await self.checkpointing.find_kafka_to_replay(operator_name, last_kafka_consumed)
        else:
            to_replay = await self.checkpointing.restore_snapshot_data(operator_name,
                                                                       deserialized_data['last_messages_processed'],
                                                                       deserialized_data['last_messages_sent'],
                                                                       last_kafka_consumed)
        for tp, offset in to_replay:
            self.kafka_ingress_consumer_pool.consumer_pool[tp].seek(tp, offset)
        logging.warning(f'finding_replays took: {(time.time_ns() // 1000000) - find_replays}')
        logging.warning(f"Snapshot restored to: {snapshot_to_restore}")

    async def start_kafka_consumer(self, topic_partitions: list[TopicPartition]):
        logging.info(f'Creating Kafka consumer per topic partitions: {topic_partitions}')
        self.kafka_ingress_consumer_pool = KafkaConsumerPool(self.id, KAFKA_URL, topic_partitions)
        await self.kafka_ingress_consumer_pool.start()
        for consumer in self.kafka_ingress_consumer_pool.consumer_pool.values():
            self.create_task(self.consumer_read(consumer))


    async def replay_from_kafka(self, operator_name, channel, offset):
        replay_until = await self.checkpointing.find_last_sent_offset(operator_name, channel)
        # logging.warning(f"replay_until: {replay_until}, offset: {offset}")
        sent_op, rec_op, partition = channel.split('_')
        # Create a kafka consumer for the given channel and seek the given offset.
        # For every kafka message, send over TCP without logging the message sent.
        count = 0
        if(replay_until is not None and replay_until > offset):
            replay_consumer = AIOKafkaConsumer(bootstrap_servers=[KAFKA_URL])
            topic_partition = TopicPartition(sent_op+rec_op, int(partition))
            replay_consumer.assign([topic_partition])
            while True: 
                # start the kafka consumer
                try:
                    await replay_consumer.start()
                    replay_consumer.seek(topic_partition, offset+1)
                except (UnknownTopicOrPartitionError, KafkaConnectionError):
                    time.sleep(1)
                    logging.warning(f'Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second')
                    continue
                break
            try:
                while True:
                    result = await replay_consumer.getone()
                    # logging.warning(f"replay_until: {replay_until}, offset: {result.offset}")
                    if replay_until is None or replay_until >= result.offset:
                        count += 1
                        await self.replay_log_message(result)
                    else:
                        break
            finally:
                # Some unclosed AIOKafkaConnection error is triggered by replay_consumer.stop()
                # logging.warning(f'Reached finally for channel {channel}')
                await replay_consumer.stop()
                # logging.warning(f'Stopped consumer for channel {channel}')
        logging.warning(f"replayed {count} messages")


    async def replay_log_message(self, msg):
        deserialized_data: dict = self.networking.decode_message(msg.value)
        # logging.warning(f'replaying msg: {deserialized_data}')
        receiver_info = deserialized_data['__MSG__']['__SENT_TO__']
        deserialized_data['__MSG__']['__SENT_FROM__']['kafka_offset'] = msg.offset
        deserialized_data['__MSG__']['__REPLAYED__'] = True
        await self.networking.replay_message(receiver_info['host'], receiver_info['port'], deserialized_data)

    async def simple_failure(self):
        await asyncio.sleep(48)
        self.snapshot_event.clear()
        self.no_failure_event.clear()
        while self.function_tasks:
            t = self.function_tasks.pop()
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass            
        logging.warning("All function are canceled.")
        await self.networking.send_message(
            DISCOVERY_HOST, DISCOVERY_PORT,
            {
                "__COM_TYPE__": 'WORKER_FAILED',
                "__MSG__": self.id
            },
            Serializer.MSGPACK
        )
        

    async def consumer_read(self, consumer: AIOKafkaConsumer):
        while True:
            await self.snapshot_event.wait()
            result = await consumer.getmany(timeout_ms=1, max_records=100)
            for _, messages in result.items():
                if messages:
                    # logging.warning('Processing kafka messages')
                    for message in messages:
                        self.handle_message_from_kafka(message)
            await asyncio.sleep(0.01)


    def handle_message_from_kafka(self, msg):
        logging.info(
            f"Consumed: {msg.topic} {msg.partition} {msg.offset} "
            f"{msg.key} {msg.value} {msg.timestamp}"
        )
        if self.checkpointing is not None:
            self.checkpointing.set_consumed_offset(msg.topic, msg.partition, msg.offset)
        deserialized_data: dict = self.networking.decode_message(msg.value)
        # This data should be added to a replay kafka topic.
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        if message_type == 'RUN_FUN':
            run_func_payload: RunFuncPayload = self.unpack_run_payload(message, msg.key, timestamp=msg.timestamp)
            logging.info(f'RUNNING FUNCTION FROM KAFKA: {run_func_payload.function_name} {run_func_payload.key}')
            if not self.notified_coordinator:
                self.notified_coordinator = True
                self.create_task(self.notify_coordinator())
                self.start_checkpointing.set()
                if self.id == 1:
                    self.create_task(self.simple_failure())

            if message['__FUN_NAME__'] == 'trigger':
                self.create_task(
                    self.run_function(
                        run_func_payload
                    )
                )
            elif self.no_failure_event.is_set():
                self.create_run_function_task(
                    self.run_function(
                        run_func_payload
                    )
                )
        else:
            logging.error(f"Invalid message type: {message_type} passed to KAFKA")

    async def notify_coordinator(self):
        await self.networking.send_message(
            DISCOVERY_HOST, DISCOVERY_PORT,
            {
                "__COM_TYPE__": 'STARTED_PROCESSING',
                "__MSG__": self.id
            },
            Serializer.MSGPACK
        )

    def create_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
    
    def create_run_function_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.function_tasks.add(task)
        task.add_done_callback(self.function_tasks.discard)

    def set_channels_dict(self, channel_list):
        for t in channel_list:
            from_op, to_op, shuffle = t
            if from_op is not None and to_op is not None:
                self.channels[from_op + to_op] = shuffle


    async def checkpoint_coordinated_sources(self, pool, sources, _round):
        for source in sources:
            # Checkpoint the source operator
            outgoing_channels = await self.checkpointing.get_outgoing_channels(source)
            # Send marker on all outgoing channels
            await self.take_snapshot(pool, source, cor_round=_round)
            # logging.warning("sending markers")
            for _id, operator in outgoing_channels:
                self.create_task(self.send_marker(source, _id, operator, _round))

    async def send_marker(self, source, _id, operator, _round):
        if _id not in self.peers.keys():
            logging.warning('Unknown id in network')
        else:
            (host, port) = self.peers[_id]
            await self.networking.send_message(
                host, port,
                {
                    "__COM_TYPE__": 'COORDINATED_MARKER',
                    "__MSG__": (self.id, source, operator, _round)
                },
                Serializer.MSGPACK
            )

    async def process_message_buffer(self, operator):
        if operator in self.message_buffer.keys():
            for message in self.message_buffer[operator]:
                payload = self.unpack_run_payload(message, message['__RQ_ID__'])
                self.create_run_function_task(self.run_function(payload, send_from=message['__SENT_FROM__']))
            self.message_buffer[operator] = []

    async def worker_controller(self, pool: concurrent.futures.ProcessPoolExecutor, deserialized_data, resp_adr):
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        match message_type:
            case 'RUN_FUN_REMOTE' | 'RUN_FUN_RQ_RS_REMOTE':
                # logging.warning(f"RQ_DI: {message['__RQ_ID__']}, operator: {message['__OP_NAME__']},before")
                if (not self.no_failure_event.is_set()) and ('__REPLAYED__' in message.keys()):
                    await self.no_failure_event.wait()
                if self.no_failure_event.is_set():
                    # logging.warning(f"RQ_DI: {message['__RQ_ID__']}, operator: {message['__OP_NAME__']},after")
                    request_id = message['__RQ_ID__']
                    if message_type == 'RUN_FUN_REMOTE':
                        sender_details = message['__SENT_FROM__']

                        if self.checkpoint_protocol == 'COR' and \
                                await self.checkpointing.check_marker_received(message['__OP_NAME__'],
                                                                            sender_details['sender_id'],
                                                                            sender_details['operator_name']):
                            # logging.warning('Buffering message!')
                            self.message_buffer[message['__OP_NAME__']].append(message)
                        else:
                            logging.info('CALLED RUN FUN FROM PEER')
                            if self.checkpoint_protocol == 'CIC':
                                oper_name = message['__OP_NAME__']
                                # CHANGE TO CIC OBJECT
                                cycle_detected, cic_clock = await self.checkpointing.cic_cycle_detection(
                                    oper_name, message['__CIC_DETAILS__'])
                                if cycle_detected:
                                    logging.warning(f'Cycle detected for operator {oper_name}! Taking forced checkpoint.')
                                    # await self.networking.update_cic_checkpoint(oper_name)
                                    self.snapshot_event.clear()
                                    await self.take_snapshot(pool, message['__OP_NAME__'], cic_clock=cic_clock)
                                    self.snapshot_event.set()
                            payload = self.unpack_run_payload(message, request_id)
                            self.create_run_function_task(
                                self.run_function(
                                    payload, send_from=sender_details
                                )
                            )
                    else:
                        logging.info('CALLED RUN FUN RQ RS FROM PEER')
                        payload = self.unpack_run_payload(message, request_id, response_socket=resp_adr)
                        self.create_run_function_task(
                            self.run_function(
                                payload
                            )
                        )
            case 'CHECKPOINT_PROTOCOL':
                self.checkpoint_protocol = message[0]
                self.checkpoint_interval = message[1]
                self.networking.set_checkpoint_protocol(message[0])
            case 'SEND_CHANNEL_LIST':
                self.channel_list = message
                self.set_channels_dict(channel_list=self.channel_list)
                self.networking.set_channels(self.channels)
            case 'GET_METRICS':
                logging.warning('METRIC REQUEST RECEIVED.')
                return_value = (self.offsets_per_second, await self.networking.get_total_network_size(), await self.networking.get_protocol_network_size())
                reply = self.networking.encode_message(return_value, Serializer.MSGPACK)
                self.router.write((resp_adr, reply))
            case 'TAKE_COORDINATED_CHECKPOINT':
                if self.checkpoint_protocol == 'COR':
                    self.snapshot_event.clear()
                    sources = await self.checkpointing.get_source_operators()
                    if len(sources) == 0:
                        logging.warning('No source operators, nothing to checkpoint.'
                                        ' Check if operator tree is supplied in generator.')
                    else:
                        await self.checkpoint_coordinated_sources(pool, sources, message)
            case 'COORDINATED_MARKER':
                all_markers_received = await self.checkpointing.marker_received(message)
                if all_markers_received:
                    await self.checkpoint_coordinated_sources(pool, [message[2]], message[3])
                    await self.process_message_buffer(message[2])
                    checkpointing_done = await self.checkpointing.set_sink_operator(message[2])
                    if checkpointing_done and self.no_failure_event.is_set():
                        self.snapshot_event.set()
                        await self.networking.send_message(
                            DISCOVERY_HOST, DISCOVERY_PORT,
                            {
                                "__COM_TYPE__": 'COORDINATED_ROUND_DONE',
                                "__MSG__": (self.id, message[3])
                            },
                            Serializer.MSGPACK
                        )
                        
            case 'RECOVER_FROM_SNAPSHOT':
                logging.warning('Recovery message received.')
                self.no_failure_event.clear()
                self.snapshot_event.clear()
                cleaning_running = time.time_ns() // 1000000
                logging.warning(f'function tasks size before canceling: {len(self.function_tasks)}')
                while self.function_tasks:
                    t = self.function_tasks.pop()
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass            
                logging.warning(f'function tasks size after canceling: {len(self.function_tasks)}')
                logging.warning(f'stopping functions took: {(time.time_ns() // 1000000) - cleaning_running}')

                match self.checkpoint_protocol:
                    case 'CIC' | 'UNC':
                        for op_name in message.keys():

                            # If timestamp is zero, it means reset from the beginning
                            # Therefore we reset the local state to an empty dict
                            if message[op_name][0] == 0:
                                async with self.snapshot_state_lock:
                                    self.local_state.data[op_name] = {}
                                    tp_to_reset = await self.checkpointing.reset_messages_processed(op_name)
                                    for tp in tp_to_reset:
                                        self.kafka_consumer.seek(tp, 0)
                            else:
                                # Build the snapshot name from the recovery message received
                                snapshot_to_restore = f'snapshot_{self.id}_{op_name}_{message[op_name][0]}.bin'
                                await self.restore_from_snapshot(snapshot_to_restore, op_name)
                            # Replay channels from corresponding offsets in message[1]
                            replay_messages_start = time.time_ns() // 1000000
                            for channel in message[op_name][1].keys():
                                logging.warning(f"started_replaying {channel}")
                                await self.replay_from_kafka(op_name, channel, message[op_name][1][channel])
                                logging.warning(f"replayed channel {channel}")
                            logging.warning(f"replaying messages took: {(time.time_ns() // 1000000) - replay_messages_start}")
                            await self.networking.send_message(
                                DISCOVERY_HOST, DISCOVERY_PORT,
                                {
                                    "__COM_TYPE__": 'RECOVERY_DONE',
                                    "__MSG__": self.id
                                },
                                Serializer.MSGPACK
                            )
                    case 'COR':
                        for op_name in self.total_partitions_per_operator.keys():
                            if message == -1:
                                async with self.snapshot_state_lock:
                                    self.local_state.data[op_name] = {}
                                    tp_to_reset = await self.checkpointing.get_partitions_to_reset(op_name)
                                    for tp in tp_to_reset:
                                        self.kafka_consumer.seek(tp, 0)
                            else:
                                # Build the snapshot name from the recovery message received
                                snapshot_to_restore = f'snapshot_{self.id}_{op_name}_{message}.bin'
                                await self.restore_from_snapshot(snapshot_to_restore, op_name)
                                await self.networking.send_message(
                                    DISCOVERY_HOST, DISCOVERY_PORT,
                                    {
                                        "__COM_TYPE__": 'RECOVERY_DONE',
                                        "__MSG__": self.id
                                    },
                                    Serializer.MSGPACK
                                )
                    case _:
                        logging.warning('Snapshot restore message received for unknown protocol, no restoration.')
                self.snapshot_event.set()
                self.no_failure_event.set()
    
            # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
            case 'RECEIVE_EXE_PLN':
                # This contains all the operators of a job assigned to this worker
                # Message that tells the worker its execution plan from round_robin.schedule
                await self.handle_execution_plan(pool, message)
                self.attach_state_to_operators()
            # ADD CASE FOR TESTING SNAPSHOT RESTORE
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    def attach_state_to_operators(self):
        operator_names: set[str] = set([operator.name for operator in self.registered_operators.values()])
        if self.operator_state_backend == LocalStateBackend.DICT:
            self.local_state = InMemoryOperatorState(operator_names)
            # self.snapshot_state_lock = self.local_state.snapshot_state_lock
            # self.snapshot_event = self.local_state.snapshot_event
            # self.no_failure_event = self.local_state.no_failure_event
        elif self.operator_state_backend == LocalStateBackend.REDIS:
            self.local_state = RedisOperatorState(operator_names)
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.networking, self.dns)

    async def send_throughputs(self):
        while(True):
            await asyncio.sleep(1)
            offsets = await self.checkpointing.get_offsets()
            for operator in offsets.keys():
                if operator not in self.offsets_per_second.keys():
                    self.offsets_per_second[operator] = {}
                for part in offsets[operator].keys():
                    if part not in self.offsets_per_second[operator].keys():
                        self.offsets_per_second[operator][part] = []
                    self.offsets_per_second[operator][part].append(offsets[operator][part])


    async def handle_execution_plan(self, pool, message):
        worker_operators, self.dns, self.peers,\
            self.operator_state_backend, self.total_partitions_per_operator, partitions_to_ids = message
        self.networking.set_id(self.id)
        self.waiting_for_exe_graph = False
        match self.checkpoint_protocol:
            case 'CIC':
                self.checkpointing = CICCheckpointing()
                await self.checkpointing.set_id(self.id)
                await self.checkpointing.init_attributes_per_operator(self.total_partitions_per_operator.keys())
            case 'UNC':
                self.checkpointing = UncoordinatedCheckpointing()
                await self.checkpointing.set_id(self.id)
                await self.checkpointing.init_attributes_per_operator(self.total_partitions_per_operator.keys())
            case 'COR':
                self.checkpointing = CoordinatedCheckpointing()
                for op in self.total_partitions_per_operator.keys():
                    self.message_buffer[op] = []
                await self.checkpointing.set_id(self.id)
            case _:
                logging.warning('Not supported value is set for CHECKPOINTING_PROTOCOL, continue without checkpoints.')
        self.networking.set_checkpointing(self.checkpointing)

        # self.create_task(self.send_throughputs())

        match self.checkpoint_protocol:
            case 'CIC':
                # CHANGE TO CIC OBJECT
                await self.checkpointing.init_cic(self.total_partitions_per_operator.keys(), self.peers.keys())
                for operator in self.total_partitions_per_operator.keys():
                    await self.checkpointing.update_cic_checkpoint(operator)
                del self.peers[self.id]
                # START CHECKPOINTING DEPENDING ON PROTOCOL
                self.create_task(self.communication_induced_checkpointing(pool, self.checkpoint_interval))
                # CHANGE TO CIC OBJECT
                await self.checkpointing.set_peers(self.peers)
            case 'UNC':
                del self.peers[self.id]
                self.create_task(self.uncoordinated_checkpointing(pool, self.checkpoint_interval))
                logging.warning("uncoordinated checkpointing started")
            case 'COR':
                await self.checkpointing.set_peers(self.peers)
                await self.checkpointing.process_channel_list(self.channel_list, worker_operators, partitions_to_ids)
            case _:
                logging.info('no checkpointing started.')
        await self.networking.set_total_partitions_per_operator(self.total_partitions_per_operator)
        operator: Operator
        for tup in worker_operators:
            operator, partition = tup
            self.registered_operators[(operator.name, partition)] = operator
            if INGRESS_TYPE == 'KAFKA':
                self.topic_partitions.append(TopicPartition(operator.name, partition))
        await self.networking.start_kafka_logging_producer_pool()
        self.create_task(self.start_kafka_consumer(self.topic_partitions))
        logging.info(
            f'Registered operators: {self.registered_operators} \n'
            f'Peers: {self.peers} \n'
            f'Operator locations: {self.dns}'
        )

    # CHECKPOINTING PROTOCOLS:

    async def uncoordinated_checkpointing(self, pool, checkpoint_interval):
        while True:
            interval_randomness = random.randint(checkpoint_interval - 1, checkpoint_interval + 1)
            await asyncio.sleep(interval_randomness)
            if self.no_failure_event.is_set() and self.start_checkpointing.is_set():
                self.snapshot_event.clear()
                for operator in self.total_partitions_per_operator.keys():
                    await self.take_snapshot(pool, operator)
                self.snapshot_event.set()

    async def communication_induced_checkpointing(self, pool, checkpoint_interval):
        while self.waiting_for_exe_graph:
            await asyncio.sleep(0.1)
        for operator in self.total_partitions_per_operator.keys():
            self.create_task(self.cic_per_operator(pool, checkpoint_interval, operator))

    async def cic_per_operator(self, pool, checkpoint_interval, operator):
        while True:
            interval_randomness = random.randint(checkpoint_interval-1, checkpoint_interval+1)
            current_time = time.time_ns() // 1000000
            last_snapshot_timestamp = await self.checkpointing.get_last_snapshot_timestamp(operator)
            if self.no_failure_event.is_set() and self.start_checkpointing.is_set():
                if current_time > last_snapshot_timestamp + (interval_randomness*1000):
                    await self.checkpointing.update_cic_checkpoint(operator)
                    cic_clock = await self.checkpointing.get_cic_logical_clock(operator)
                    self.snapshot_event.clear()
                    await self.take_snapshot(pool, operator, cic_clock=cic_clock)
                    self.snapshot_event.set()
                    await asyncio.sleep(interval_randomness)
                else:
                    await asyncio.sleep(
                        ceil(((last_snapshot_timestamp + (checkpoint_interval*1000)) - current_time) / 1000)
                    )
            else:
                await asyncio.sleep(interval_randomness)


    async def start_tcp_service(self):
        self.router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")
        self.kafka_egress_producer_pool = KafkaProducerPool(self.id, KAFKA_URL, size=100)
        await self.kafka_egress_producer_pool.start()
        logging.info(
            f"Worker TCP Server listening at 0.0.0.0:{SERVER_PORT} "
            f"IP:{self.networking.host_name}"
        )
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            try:
                while True:
                    # This is where we read from TCP, log at receiver
                    resp_adr, data = await self.router.read()
                    deserialized_data: dict = self.networking.decode_message(data)
                    if '__COM_TYPE__' not in deserialized_data:
                        logging.error("Deserialized data do not contain a message type")
                    else:
                        self.create_task(self.worker_controller(pool, deserialized_data, resp_adr))
            finally:
                await self.kafka_egress_producer_pool.close()

    @staticmethod
    def unpack_run_payload(
            message: dict, request_id: bytes,
            timestamp=None, response_socket=None
    ) -> RunFuncPayload:
        timestamp = message['__TIMESTAMP__'] if timestamp is None else timestamp
        return RunFuncPayload(
            request_id, message['__KEY__'], timestamp,
            message['__OP_NAME__'], message['__PARTITION__'],
            message['__FUN_NAME__'], tuple(message['__PARAMS__']), response_socket
        )

    async def register_to_coordinator(self):
        self.id = await self.networking.send_message_request_response(
            DISCOVERY_HOST, DISCOVERY_PORT,
            {
                "__COM_TYPE__": 'REGISTER_WORKER',
                "__MSG__": self.networking.host_name
            },
            Serializer.MSGPACK
        )
        logging.info(f"Worker id: {self.id}")

    async def main(self):
        await self.register_to_coordinator()
        await self.start_tcp_service()
        self.kafka_ingress_consumer_pool.close()


if __name__ == "__main__":
    uvloop.install()
    worker = Worker()
    asyncio.run(Worker().main())
