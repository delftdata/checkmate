import asyncio
import dataclasses
import struct
import socket
import time
import os
import sys

import zmq
from aiozmq import create_zmq_stream, ZmqStream
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaConnectionError

from universalis.common.kafka_producer_pool import KafkaProducerPool

from .logging import logging
from .serialization import Serializer, msgpack_serialization, msgpack_deserialization, \
    cloudpickle_serialization, cloudpickle_deserialization, pickle_serialization, pickle_deserialization

KAFKA_URL: str = os.getenv('KAFKA_URL', None)


@dataclasses.dataclass
class SocketConnection:
    zmq_socket: ZmqStream
    socket_lock: asyncio.Lock


class SocketPool:

    def __init__(self, host: str, port: int, size: int = 4):
        self.host = host
        self.port = port
        self.size = size
        self.conns: list[SocketConnection] = []
        self.index: int = 0

    def __iter__(self):
        return self

    def __next__(self) -> SocketConnection:
        conn = self.conns[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn

    async def create_socket_connections(self):
        for _ in range(self.size):
            soc = await create_zmq_stream(zmq.DEALER, connect=f"tcp://{self.host}:{self.port}")
            self.conns.append(SocketConnection(soc, asyncio.Lock()))

    def close(self):
        for conn in self.conns:
            conn.zmq_socket.close()
            if conn.socket_lock.locked():
                conn.socket_lock.release()
        self.conns = []


class NetworkingManager:

    kafka_producer: AIOKafkaProducer

    def __init__(self):
        self.pools: dict[tuple[str, int], SocketPool] = {}  # HERE BETTER TO ADD A CONNECTION POOL
        self.get_socket_lock = asyncio.Lock()
        self.get_last_message_lock = asyncio.Lock()
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        self.last_messages_sent = {}
        self.total_partitions_per_operator = {}

        self.total_network_size = 0
        self.additional_cic_size = 0
        self.additional_coordinated_size = 0
        self.additional_uncoordinated_size = 0

        self.id = -1

        self.checkpointing = None
        self.checkpoint_protocol = None

        self.kafka_logging_producer_pool = KafkaProducerPool(self.id, KAFKA_URL, size=100)
        self.message_logging = set()

    def set_id(self, id):
        self.id = id

    def set_checkpoint_protocol(self, protocol):
        self.checkpoint_protocol = protocol

    def set_checkpointing(self, checkpointing):
        self.checkpointing = checkpointing

    async def get_total_network_size(self):
        return self.total_network_size

    async def get_protocol_network_size(self):
        match self.checkpoint_protocol:
            case 'COR':
                return self.additional_coordinated_size
            case 'CIC':
                return self.additional_cic_size
            case 'UNC':
                return self.additional_uncoordinated_size
            case _:
                return 0

    async def start_kafka_producer(self):
        # Set the batch_size and linger_ms to a high number and use manual flushes to commit to kafka.
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=[KAFKA_URL],
                                               enable_idempotence=True,
                                               acks='all')
        while True:
            try:
                await self.kafka_producer.start()
            except KafkaConnectionError:
                time.sleep(1)
                logging.info("Waiting for Kafka networking producer")
                continue
            break
        logging.info('KAFKA PRODUCER STARTED FOR NETWORKING')

    async def start_kafka_logging_producer_pool(self):
        await self.kafka_logging_producer_pool.start()

    async def start_kafka_logging_sync_producer_pool(self):
        self.kafka_logging_producer_pool.start_sync()

    async def flush_kafka_buffer(self, operator):
        await self.kafka_producer.flush()
        last_msg_sent = self.last_messages_sent[operator]
        self.last_messages_sent[operator] = {}
        return last_msg_sent

    async def flush_kafka_producer_pool(self, operator):
        kafka_flushes = [kafka_producer.flush() for kafka_producer in self.kafka_logging_producer_pool.producer_pool]
        await asyncio.gather(*kafka_flushes)
        kafka_flushes = []
        await asyncio.gather(*self.message_logging)
        self.message_logging = set()
        last_msg_sent = self.last_messages_sent[operator]
        self.last_messages_sent[operator] = {}
        return last_msg_sent

    async def stop_kafka_producer(self):
        await self.kafka_producer.stop()

    async def set_total_partitions_per_operator(self, par_per_op):
        self.total_partitions_per_operator = par_per_op
        for key in par_per_op.keys():
            self.last_messages_sent[key] = {}

    async def close_all_connections(self):
        async with self.get_socket_lock:
            for pool in self.pools.values():
                pool.close()
            self.pools = {}

    async def create_socket_connection(self, host: str, port):
        self.pools[(host, port)] = SocketPool(host, port)
        await self.pools[(host, port)].create_socket_connections()

    def close_socket_connection(self, host: str, port):
        if (host, port) in self.pools:
            self.pools[(host, port)].close()
        else:
            logging.warning('The socket that you are trying to close does not exist')

    async def log_message(self,
                          msg,
                          send_part,
                          send_name,
                          rec_part,
                          rec_name,
                          serializer: Serializer = Serializer.PICKLE):
        logging_partition=send_part*(self.total_partitions_per_operator[rec_name]) + rec_part
        kafka_future = await self.kafka_logging_producer_pool.pick_producer(
            logging_partition).send_and_wait(send_name+rec_name,
                                             value=self.encode_message(msg, serializer),
                                             partition=logging_partition)
        channel_key = f'{send_name}_{rec_name}_{logging_partition}'
        if channel_key not in self.last_messages_sent[send_name] or \
                kafka_future.offset > self.last_messages_sent[send_name][channel_key]:
            self.last_messages_sent[send_name][channel_key] = kafka_future.offset
        return kafka_future.offset

    async def send_message(self,
                           host,
                           port,
                           msg: dict[str, object],
                           serializer: Serializer = Serializer.CLOUDPICKLE,
                           sending_name=None,
                           sending_partition=None):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        sender_details = {
            'operator_name': sending_name,
            'operator_partition': sending_partition,
            'sender_id': self.id,
            'kafka_offset': None
        }
        receiver_details = {
            'host': host,
            'port': port
        }
        if msg['__COM_TYPE__'] == 'RUN_FUN_REMOTE':
            async with self.get_last_message_lock:
                msg['__MSG__']['__SENT_FROM__'] = sender_details
                msg['__MSG__']['__SENT_TO__'] = receiver_details
                if self.checkpoint_protocol in ['UNC', 'CIC']:
                    receiving_name = msg['__MSG__']['__OP_NAME__']
                    receiving_partition = msg['__MSG__']['__PARTITION__']
                    task = asyncio.create_task(self.log_message(msg,
                                                                sending_partition,
                                                                sending_name,
                                                                receiving_partition,
                                                                receiving_name))
                    self.message_logging.add(task)
                    task.add_done_callback(self.message_logging.discard)
                    await task
                    msg['__MSG__']['__SENT_FROM__']['kafka_offset'] = task.result()
                    if self.checkpoint_protocol == 'CIC':
                        msg['__MSG__']['__CIC_DETAILS__'] = await self.checkpointing.get_message_details(host,
                                                                                                         port,
                                                                                                         sending_name,
                                                                                                         msg['__MSG__']['__OP_NAME__'])
                        self.additional_cic_size += cloudpickle_serialization('__CIC_DETAILS__').__sizeof__()
                        self.additional_cic_size += cloudpickle_serialization(msg['__MSG__']['__CIC_DETAILS__']).__sizeof__()
                new_msg = self.encode_message(msg, serializer)
                self.total_network_size += new_msg.__sizeof__()
                socket_conn.zmq_socket.write((new_msg, ))
        elif msg['__COM_TYPE__'] == 'SNAPSHOT_TAKEN':
            new_msg = self.encode_message(msg, serializer)
            self.total_network_size += new_msg.__sizeof__()
            size = new_msg.__sizeof__()
            # logging.warning(f"snapshot_taken size:{size}")
            self.additional_uncoordinated_size += size
            self.additional_cic_size += size
            socket_conn.zmq_socket.write((new_msg, ))
        elif msg['__COM_TYPE__'] in ['COORDINATED_MARKER', 'COORDINATED_ROUND_DONE', 'TAKE_COORDINATED_CHECKPOINT']:
            new_msg = self.encode_message(msg, serializer)
            self.total_network_size += new_msg.__sizeof__()
            self.additional_coordinated_size += new_msg.__sizeof__()
            socket_conn.zmq_socket.write((new_msg, ))
        else:
            new_msg = self.encode_message(msg, serializer)
            self.total_network_size += new_msg.__sizeof__()
            socket_conn.zmq_socket.write((new_msg, ))


    async def replay_message(self,
                             host,
                             port,
                             msg,
                             serializer: Serializer = Serializer.PICKLE):
        # Replays a message without logging anything
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        msg['__MSG__']['__CIC_DETAILS__'] = {}
        if self.checkpoint_protocol == 'CIC':
            msg['__MSG__']['__CIC_DETAILS__'] = await self.checkpointing.get_message_details(host, port, msg['__MSG__']['__SENT_FROM__']['operator_name'], \
                                                                                             msg['__MSG__']['__OP_NAME__'])

        msg = self.encode_message(msg, serializer)
        if self.checkpoint_protocol == 'CIC':
            self.additional_cic_size += msg.__sizeof__()
        elif self.checkpoint_protocol == 'UNC':
            self.additional_uncoordinated_size += msg.__sizeof__()
        self.total_network_size += msg.__sizeof__()
        socket_conn.zmq_socket.write((msg, ))

    async def __receive_message(self, sock):
        # To be used only by the request response because the lock is needed
        answer = await sock.read()
        return self.decode_message(answer[0])

    async def send_message_request_response(self,
                                            host,
                                            port,
                                            msg: dict[str, object],
                                            serializer: Serializer = Serializer.CLOUDPICKLE):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        async with socket_conn.socket_lock:
            await self.__send_message_given_sock(socket_conn.zmq_socket, msg, serializer)
            resp = await self.__receive_message(socket_conn.zmq_socket)
            logging.info("NETWORKING MODULE RECEIVED RESPONSE")
            return resp

    async def __send_message_given_sock(self, sock, msg, serializer):
        msg = self.encode_message(msg, serializer)
        sock.write((msg, ))

    @staticmethod
    def encode_message(msg: object, serializer: Serializer) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack('>H', 0) + cloudpickle_serialization(msg)
            return msg
        elif serializer == Serializer.MSGPACK:
            msg = struct.pack('>H', 1) + msgpack_serialization(msg)
            return msg
        elif serializer == Serializer.PICKLE:
            msg = struct.pack('>H', 2) + pickle_serialization(msg)
            return msg
        else:
            logging.info(f'Serializer: {serializer} is not supported')

    @staticmethod
    def decode_message(data):
        serializer = struct.unpack('>H', data[0:2])[0]
        if serializer == 0:
            return cloudpickle_deserialization(data[2:])
        elif serializer == 1:
            return msgpack_deserialization(data[2:])
        elif serializer == 2:
            return pickle_deserialization(data[2:])
        else:
            logging.info(f'Serializer: {serializer} is not supported')
