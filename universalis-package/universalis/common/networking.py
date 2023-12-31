import asyncio
import dataclasses
import io
import shutil
import struct
import socket
import os
from typing import BinaryIO

import zmq
from aiozmq import create_zmq_stream, ZmqStream

from .logging import logging
from .serialization import Serializer, msgpack_serialization, msgpack_deserialization, \
    cloudpickle_serialization, cloudpickle_deserialization, pickle_serialization, pickle_deserialization

KAFKA_URL: str = os.getenv('KAFKA_URL', None)
UNC_LOG_PATH: str = os.getenv('UNC_LOG_PATH', '/usr/local/universalis/unc-logs')


@dataclasses.dataclass
class SocketConnection(object):
    zmq_socket: ZmqStream
    socket_lock: asyncio.Lock


class SocketPool(object):

    def __init__(self, host: str, port: int, size: int = 4):
        self.host = host
        self.port = port
        self.size = size
        self.conns: list[SocketConnection] = []
        self.index: int = 0
        self.push_conns: list[ZmqStream] = []
        self.push_index: int = 0

    def get_next_dealer_conn(self) -> SocketConnection:
        conn = self.conns[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn

    def get_next_push_conn(self) -> ZmqStream:
        conn = self.push_conns[self.push_index]
        next_idx = self.push_index + 1
        self.push_index = 0 if next_idx == self.size else next_idx
        return conn

    async def create_socket_connections(self):
        for _ in range(self.size):
            soc = await create_zmq_stream(zmq.DEALER, connect=f"tcp://{self.host}:{int(self.port) + 1}",
                                          high_read=0, high_write=0)
            soc.transport.setsockopt(zmq.LINGER, 0)
            self.conns.append(SocketConnection(soc, asyncio.Lock()))
            push_soc = await create_zmq_stream(zmq.PUSH, connect=f"tcp://{self.host}:{self.port}",
                                               high_read=0, high_write=0)
            push_soc.transport.setsockopt(zmq.LINGER, 0)
            self.push_conns.append(push_soc)

    def close(self):
        for conn in self.conns:
            conn.zmq_socket.close()
            if conn.socket_lock.locked():
                conn.socket_lock.release()
        for conn in self.push_conns:
            conn.close()
        self.conns = []
        self.push_conns = []


class NetworkingManager(object):

    def __init__(self):
        self.pools: dict[tuple[str, int], SocketPool] = {}
        self.get_socket_lock = asyncio.Lock()
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        self.total_partitions_per_operator = {}

        self.total_network_size = 0
        self.additional_cic_size = 0
        self.additional_coordinated_size = 0
        self.additional_uncoordinated_size = 0

        self.id = -1

        self.checkpointing = None
        self.checkpoint_protocol = None

        self.log_file_descriptors: dict[str, BinaryIO | None] = {}
        self.log_file_offsets: dict[str, int] = {}
        self.log_file_index: dict[str, list[str]] = {}
        self.byte_offsets: dict[str, dict[int, tuple[int, int]]] = {}

        self.log_file_paths: dict[str, str] = {}
        self.recovery_cycle: int = 0
        self.registered_ops = None

    def set_id(self, _id):
        self.id = _id

    def set_checkpoint_protocol(self, protocol):
        self.checkpoint_protocol = protocol

    def set_checkpointing(self, checkpointing):
        self.checkpointing = checkpointing

    def get_total_network_size(self):
        return self.total_network_size

    def increment_recovery_cycle(self):
        self.recovery_cycle += 1

    def get_protocol_network_size(self):
        if self.checkpoint_protocol == 'COR':
            return self.additional_coordinated_size
        elif self.checkpoint_protocol == 'CIC':
            return self.additional_cic_size
        elif self.checkpoint_protocol == 'UNC':
            return self.additional_uncoordinated_size
        return 0

    def init_log_files(self,
                       channel_list: list[tuple[str, str, bool]],
                       registered_operators: list[tuple[str, int]]):
        # Create the log files and populate the log_file_descriptors and log_file_offsets

        self.registered_ops = registered_operators

        worker_log_path = os.path.join(UNC_LOG_PATH, f'worker-{self.id}-logs')
        if os.path.exists(worker_log_path):
            shutil.rmtree(worker_log_path)
        os.makedirs(worker_log_path)
        for t in channel_list:
            from_op, to_op, shuffle = t
            if to_op is None or from_op not in self.total_partitions_per_operator:
                # if it's sink it does not have log files
                continue
            for i in range(self.total_partitions_per_operator[from_op]):
                if (from_op, i) not in registered_operators:
                    # if this operator partition is not in this worker, no need to create its log files
                    continue
                if shuffle:
                    # The operator communicates with all the rest
                    for j in range(self.total_partitions_per_operator[to_op]):
                        tmp_str = (i * self.total_partitions_per_operator[to_op] + j)
                        file_name = f'{from_op}_{to_op}_{tmp_str}'
                        self.log_file_paths[file_name] = os.path.join(worker_log_path, file_name) + '.bin'
                        self.log_file_descriptors[file_name] = None
                        self.log_file_offsets[file_name] = 0
                        self.byte_offsets[file_name] = {}
                        if from_op in self.log_file_index:
                            self.log_file_index[from_op].append(file_name)
                        else:
                            self.log_file_index[from_op] = [file_name]
                else:
                    # The operator communicates with only one
                    if i >= self.total_partitions_per_operator[to_op]:
                        # if this operator partition is not in this worker, no need to create its log files
                        continue
                    tmp_str = (i * self.total_partitions_per_operator[to_op] + i)
                    file_name = f'{from_op}_{to_op}_{tmp_str}'
                    self.log_file_paths[file_name] = os.path.join(worker_log_path, file_name) + '.bin'
                    self.log_file_descriptors[file_name] = None
                    self.log_file_offsets[file_name] = 0
                    self.byte_offsets[file_name] = {}
                    if from_op in self.log_file_index:
                        self.log_file_index[from_op].append(file_name)
                    else:
                        self.log_file_index[from_op] = [file_name]
        self.open_logfiles_for_append()

    def close_logfiles(self):
        for log_file in self.log_file_descriptors.values():
            log_file.close()

    def open_logfiles_for_read(self):
        for log_file_name in self.log_file_descriptors:
            self.log_file_descriptors[log_file_name] = open(self.log_file_paths[log_file_name], 'rb')

    def open_logfiles_for_append(self):
        for log_file_name in self.log_file_descriptors:
            self.log_file_descriptors[log_file_name] = open(self.log_file_paths[log_file_name], 'ab')

    async def set_total_partitions_per_operator(self, par_per_op):
        self.total_partitions_per_operator = par_per_op

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

    def log_message(self,
                    msg,
                    send_part,
                    send_name,
                    rec_part,
                    rec_name,
                    serializer: Serializer = Serializer.PICKLE):

        try:
            message = self.encode_message(msg, serializer)
            tmp_str = (send_part * self.total_partitions_per_operator[rec_name] + rec_part)
            logfile_name = f'{send_name}_{rec_name}_{tmp_str}'
            # log message to file
            offset = self.log_file_descriptors[logfile_name].tell()
            try:
                bytes_written = self.log_file_descriptors[logfile_name].write(message)
            except io.UnsupportedOperation:
                logging.warning('Should not have tried to write to the logs at recovery time')
            else:
                self.log_file_descriptors[logfile_name].flush()
                # log the byte offsets of the message
                self.byte_offsets[logfile_name][self.log_file_offsets[logfile_name]] = (offset, bytes_written)
                # increase the message offset for the next message
                offset = self.log_file_offsets[logfile_name]
                self.log_file_offsets[logfile_name] += 1
                return offset
        except KeyError:
            logging.warning(f'File: {logfile_name} does not exist in worker: {self.id} with operators:'
                            f' {self.registered_ops} with message: {msg}')

    def log_seek(self,
                 log_file_name,
                 msg_offset):
        file_offset = self.byte_offsets[log_file_name][msg_offset][0]
        self.log_file_descriptors[log_file_name].seek(file_offset)

    def get_log_line(self,
                     log_file_name,
                     msg_offset):
        file_offset = self.byte_offsets[log_file_name][msg_offset][0]
        self.log_file_descriptors[log_file_name].seek(file_offset)
        try:
            message = self.decode_message(self.log_file_descriptors[log_file_name].read(
                self.byte_offsets[log_file_name][msg_offset][1]))
        except io.UnsupportedOperation:
            logging.warning('Should not have tried to read from the logs at non-recovery time')
        else:
            return message

    def return_last_offset(self, operator) -> dict[str, int] | None:
        if operator not in self.log_file_index:
            # NOTE: check with recovery line (we might need to return 0 offsets)
            return {}
        file_names = self.log_file_index[operator]
        last_offsets = {file_name: self.log_file_offsets[file_name] - 1 for file_name in file_names}
        return last_offsets

    # TODO Change default serializer
    async def send_message(self,
                           host,
                           port,
                           msg: dict[str, object | dict],
                           serializer: Serializer = Serializer.CLOUDPICKLE,
                           sending_name=None,
                           sending_partition=None):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = self.pools[(host, port)].get_next_push_conn()
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
            msg['__MSG__']['__SENT_FROM__'] = sender_details
            msg['__MSG__']['__SENT_TO__'] = receiver_details
            msg['__MSG__']['__REC_CYC__'] = self.recovery_cycle
            if self.checkpoint_protocol in ['UNC', 'CIC']:
                receiving_name = msg['__MSG__']['__OP_NAME__']
                receiving_partition = msg['__MSG__']['__PARTITION__']
                msg['__MSG__']['__SENT_FROM__']['kafka_offset'] = self.log_message(msg,
                                                                                   sending_partition,
                                                                                   sending_name,
                                                                                   receiving_partition,
                                                                                   receiving_name)
                if self.checkpoint_protocol == 'CIC':
                    msg['__MSG__']['__CIC_DETAILS__'] = self.checkpointing.get_message_details(host,
                                                                                               port,
                                                                                               sending_name,
                                                                                               receiving_name)
                    # self.additional_cic_size += cloudpickle_serialization('__CIC_DETAILS__').__sizeof__()
                    # self.additional_cic_size += cloudpickle_serialization(msg['__MSG__']
                    #                                                       ['__CIC_DETAILS__']).__sizeof__()
            new_msg = self.encode_message(msg, serializer)
            # self.total_network_size += new_msg.__sizeof__()
            socket_conn.write((new_msg,))
        elif msg['__COM_TYPE__'] == 'SNAPSHOT_TAKEN':
            new_msg = self.encode_message(msg, serializer)
            # self.total_network_size += new_msg.__sizeof__()
            # size = new_msg.__sizeof__()
            # logging.warning(f"snapshot_taken size:{size}")
            # self.additional_uncoordinated_size += size
            # self.additional_cic_size += size
            socket_conn.write((new_msg,))
        elif msg['__COM_TYPE__'] in ['COORDINATED_MARKER', 'COORDINATED_ROUND_DONE', 'TAKE_COORDINATED_CHECKPOINT']:
            new_msg = self.encode_message(msg, serializer)
            # self.total_network_size += new_msg.__sizeof__()
            # self.additional_coordinated_size += new_msg.__sizeof__()
            socket_conn.write((new_msg,))
        else:
            new_msg = self.encode_message(msg, serializer)
            # self.total_network_size += new_msg.__sizeof__()
            socket_conn.write((new_msg,))

    async def replay_message(self,
                             host,
                             port,
                             msg,
                             serializer: Serializer = Serializer.PICKLE):
        # Replays a message without logging anything
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = self.pools[(host, port)].get_next_push_conn()
        msg['__MSG__']['__CIC_DETAILS__'] = {}
        if self.checkpoint_protocol == 'CIC':
            msg['__MSG__']['__CIC_DETAILS__'] = self.checkpointing.get_message_details(host,
                                                                                       port,
                                                                                       msg['__MSG__']
                                                                                       ['__SENT_FROM__']
                                                                                       ['operator_name'],
                                                                                       msg['__MSG__']
                                                                                       ['__OP_NAME__'])
        msg = self.encode_message(msg, serializer)
        # if self.checkpoint_protocol == 'CIC':
        #     self.additional_cic_size += msg.__sizeof__()
        # elif self.checkpoint_protocol == 'UNC':
        #     self.additional_uncoordinated_size += msg.__sizeof__()
        # self.total_network_size += msg.__sizeof__()
        socket_conn.write((msg,))

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
            socket_conn = self.pools[(host, port)].get_next_dealer_conn()
        async with socket_conn.socket_lock:
            await self.__send_message_given_sock(socket_conn.zmq_socket, msg, serializer)
            resp = await self.__receive_message(socket_conn.zmq_socket)
            logging.info("NETWORKING MODULE RECEIVED RESPONSE")
            return resp

    async def __send_message_given_sock(self, sock, msg, serializer):
        msg = self.encode_message(msg, serializer)
        sock.write((msg,))

    @staticmethod
    def encode_message(msg: object, serializer: Serializer) -> bytes | None:
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
            logging.error(f'Serializer: {serializer} is not supported')
            return None

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
            logging.error(f'Serializer: {serializer} is not supported')
            return None
