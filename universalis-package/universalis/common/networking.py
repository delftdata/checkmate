import asyncio
import dataclasses
import struct
import socket
import time
import os

import zmq
from aiozmq import create_zmq_stream, ZmqStream
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaConnectionError

from .logging import logging
from .serialization import Serializer, msgpack_serialization, msgpack_deserialization, \
    cloudpickle_serialization, cloudpickle_deserialization

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
            conn.socket_lock.release()
        self.conns = []


class NetworkingManager:

    kafka_producer: AIOKafkaProducer

    def __init__(self):
        self.pools: dict[tuple[str, int], SocketPool] = {}  # HERE BETTER TO ADD A CONNECTION POOL
        self.get_socket_lock = asyncio.Lock()
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        self.last_messages_sent = {}
        self.total_partitions_per_operator = {}

        self.id = -1
        self.peers = {}

        self.checkpoint_protocol = None

        # CIC
        self.sent_to = {}
        self.logical_clock = {}
        self.checkpoint_clocks = {}
        self.taken = {}
        self.greater = {}

    def set_checkpoint_protocol(self, protocol):
        self.checkpoint_protocol = protocol

    def set_id(self, id):
        self.id = id

    def set_peers(self, peers):
        self.peers = peers

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
        logging.info(f'KAFKA PRODUCER STARTED FOR NETWORKING')

    async def flush_kafka_buffer(self, operator):
        last_msg_sent = self.last_messages_sent[operator]
        self.last_messages_sent[operator] = {}
        await self.kafka_producer.flush()
        return last_msg_sent

    async def stop_kafka_producer(self):
        await self.kafka_producer.stop()

    async def set_total_partitions_per_operator(self, par_per_op):
        self.total_partitions_per_operator = par_per_op
        for key in par_per_op.keys():
            self.last_messages_sent[key] = {}

    async def init_cic(self, operators, ids):
            for op in operators:
                # CIC
                # Normally the algorithm uses three arrays (two bool, one clock) and a logical clock per worker.
                # However our checkpoints are operator based, meaning we need this for every operator separately.
                self.sent_to[op] = {}
                self.logical_clock[op] = 0
                self.taken[op] = {}
                self.greater[op] = {}
                self.checkpoint_clocks[op] = {}
                for id in ids:
                    self.sent_to[op][id] = {}
                    self.taken[op][id] = {}
                    self.greater[op][id] = {}
                    self.checkpoint_clocks[op][id] = {}
                    for op2 in operators:
                        self.sent_to[op][id][op2] = False
                        self.taken[op][id][op2] = False
                        self.greater[op][id][op2] = False
                        self.checkpoint_clocks[op][id][op2] = 0

    async def update_cic_checkpoint(self, operator):
        for id in self.sent_to[operator].keys():
            for op in self.sent_to[operator][id].keys():
                self.sent_to[operator][id][op] = False
                if not (op == operator and self.id == id):
                    self.taken[operator][id][op] = True
                    self.greater[operator][id][op] = True
        self.logical_clock[operator] = self.logical_clock[operator] + 1
        self.checkpoint_clocks[operator][self.id][operator] = self.checkpoint_clocks[operator][self.id][operator] + 1
        return self.logical_clock[operator]

    async def get_worker_id(self, host, port):
        worker_id = self.id
        for id in self.peers.keys():
            if (host, port) == self.peers[id]:
                worker_id = id
                break
        return worker_id
    
    async def cic_cycle_detection(self, operator, cic_details):
        if cic_details == {}:
            return False
        cycle_detected = False
        sent_greater_and = False
        for id in self.sent_to[operator].keys():
            for op in self.sent_to[operator][id].keys():
                if self.sent_to[operator][id][op] and cic_details['__GREATER__'][id][op]:
                    sent_greater_and = True
                    break
            if sent_greater_and:
                break
        if (sent_greater_and and cic_details['__LC__'] > self.logical_clock) or (cic_details['__CHECKPOINT_CLOCKS__'][self.id][operator] == self.checkpoint_clocks[operator][self.id][operator] and cic_details['__TAKEN__'][self.id][operator]):
            cycle_detected = True
        
        # Compare local clocks

        if cic_details['__LC__'] > self.logical_clock[operator]:
            self.logical_clock[operator] = cic_details['__LC__']
            self.greater[operator][self.id][operator] = False
            for id in self.greater[operator].keys():
                for op in self.greater[operator][id].keys():
                    if not (id == self.id and op == operator):
                        self.greater[operator][id][op] = cic_details['__GREATER__'][id][op]
        elif cic_details['__LC__'] == self.logical_clock[operator]:
            for id in self.greater[operator].keys():
                for op in self.greater[operator][id].keys():
                    self.greater[operator][id][op] = self.greater[operator][id][op] and cic_details['__GREATER__'][id][op]

        # Compare checkpoint clocks

        for id in cic_details['__CHECKPOINT_CLOCKS__'].keys():
            for op in cic_details['__CHECKPOINT_CLOCKS__'][id].keys():
                if id == self.id and op == operator:
                    continue
                else:
                    if cic_details['__CHECKPOINT_CLOCKS__'][id][op] > self.checkpoint_clocks[operator][id][op]:
                        self.checkpoint_clocks[operator][id][op] = cic_details['__CHECKPOINT_CLOCKS__'][id][op]
                        self.taken[operator][id][op] = cic_details['__TAKEN__'][id][op]
                    elif cic_details['__CHECKPOINT_CLOCKS__'][id][op] == self.checkpoint_clocks[operator][id][op]:
                        self.taken[operator][id][op] = cic_details['__TAKEN__'][id][op] and self.taken[operator][id][op]

        return cycle_detected

    def close_all_connections(self):
        for pool in self.pools.values():
            pool.close()

    async def create_socket_connection(self, host: str, port):
        self.pools[(host, port)] = SocketPool(host, port)
        await self.pools[(host, port)].create_socket_connections()

    def close_socket_connection(self, host: str, port):
        if (host, port) in self.pools:
            self.pools[(host, port)].close()
        else:
            logging.warning('The socket that you are trying to close does not exist')

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
            'kafka_offset': None
        }
        receiver_details = {
            'host': host,
            'port': port
        }
        if msg['__COM_TYPE__'] == 'RUN_FUN_REMOTE':
            msg['__MSG__']['__CIC_DETAILS__'] = {}
            if self.checkpoint_protocol == 'CIC':
                receiver_id = await self.get_worker_id(host, port)
                self.sent_to[sending_name][receiver_id][msg['__MSG__']['__OP_NAME__']] = True
                msg['__MSG__']['__CIC_DETAILS__']['__LC__'] = self.logical_clock[sending_name]
                msg['__MSG__']['__CIC_DETAILS__']['__GREATER__'] = self.greater[sending_name]
                msg['__MSG__']['__CIC_DETAILS__']['__TAKEN__'] = self.taken[sending_name]
                msg['__MSG__']['__CIC_DETAILS__']['__CHECKPOINT_CLOCKS__'] = self.checkpoint_clocks[sending_name]
            msg['__MSG__']['__SENT_FROM__'] = sender_details
            msg['__MSG__']['__SENT_TO__'] = receiver_details
            receiving_name = msg['__MSG__']['__OP_NAME__']
            receiving_partition = msg['__MSG__']['__PARTITION__']
            kafka_data = await self.kafka_producer.send_and_wait(sending_name+receiving_name,
                                                      value=self.encode_message(msg, serializer),
                                                      partition=sending_partition*(self.total_partitions_per_operator[receiving_name]) + receiving_partition)
            msg['__MSG__']['__SENT_FROM__']['kafka_offset'] = kafka_data.offset
            self.last_messages_sent[sending_name][sending_name+'_'+receiving_name+'_'+str(sending_partition*(self.total_partitions_per_operator[receiving_name]) + receiving_partition)] = kafka_data.offset
        msg = self.encode_message(msg, serializer)
        socket_conn.zmq_socket.write((msg, ))

    async def replay_message(self,
                             host,
                             port,
                             msg,
                             serializer: Serializer = Serializer.CLOUDPICKLE):
        # Replays a message without logging anything
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        msg = self.encode_message(msg, serializer)
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
        else:
            logging.info(f'Serializer: {serializer} is not supported')

    @staticmethod
    def decode_message(data):
        serializer = struct.unpack('>H', data[0:2])[0]
        if serializer == 0:
            return cloudpickle_deserialization(data[2:])
        elif serializer == 1:
            return msgpack_deserialization(data[2:])
        else:
            logging.info(f'Serializer: {serializer} is not supported')
