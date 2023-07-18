import asyncio
import traceback
import uuid
from typing import Awaitable, Type

from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager

from .serialization import Serializer
from .function import Function
from .base_state import BaseOperatorState as State


class StrKeyNotUUID(Exception):
    pass


class NonSupportedKeyType(Exception):
    pass


class StateNotAttachedError(Exception):
    pass


def make_key_hashable(key) -> int:
    if isinstance(key, int):
        return key
    elif isinstance(key, str):
        try:
            return uuid.UUID(key).int  # uuid type given by the user
        except ValueError:
            return uuid.uuid5(uuid.NAMESPACE_DNS, key).int  # str that we hash to SHA-1
    raise NonSupportedKeyType()  # if not int, str or uuid throw exception


class StatefulFunction(Function):

    def __init__(self,
                 key,
                 operator_name: str,
                 operator_state: State,
                 networking: NetworkingManager,
                 timestamp: int,
                 dns: dict[str, dict[str, tuple[str, int]]],
                 request_id: str):
        super().__init__()
        self.__operator_name = operator_name
        self.__state: State = operator_state
        self.__networking: NetworkingManager = networking
        self.__timestamp: int = timestamp
        self.__dns: dict[str, dict[str, tuple[str, int]]] = dns
        self.__request_id: str = request_id
        self.__async_remote_calls: list[tuple[str, str, object, tuple]] = []
        self.__key = key

    async def __call__(self, *args, **kwargs):
        try:
            res = await self.run(*args)
            await self.__send_async_calls()
        except Exception as e:
            logging.error(traceback.format_exc())
            return e
        else:
            return res

    @property
    def key(self):
        return self.__key

    async def run(self, *args):
        raise NotImplementedError
    
    @property
    def lock(self):
        return self.__state.get_lock(self.key, self.__operator_name)
    
    @property
    def operator_lock(self):
        return self.__state.get_operator_lock(self.key, self.__operator_name)

    async def get(self):
        value = await self.__state.get(self.key, self.__operator_name)
        logging.info(f'GET: {self.key}:{value} from operator: {self.__operator_name}')
        return value

    async def put(self, value):
        logging.info(f'PUT: {self.key}:{value} in operator: {self.__operator_name}')
        await self.__state.put(self.key, value, self.__operator_name)

    async def get_operator_state(self):
        state = await self.__state.get_operator_state(self.__operator_name)
        return state
    
    async def clean_operator_state(self):
        await self.__state.snapshot_event.wait()
        await self.__state.no_failure_event.wait()
        await self.__state.clean_operator_state(self.__operator_name)


    async def __send_async_calls(self):
        n_remote_calls: int = len(self.__async_remote_calls)
        if n_remote_calls > 0:
            remote_calls: list[Awaitable] = [self.call_remote_function_no_response(*entry)
                                             for entry in self.__async_remote_calls]
            logging.info(f'Sending chain calls for function: {self.name} with remote call number: {n_remote_calls}'
                         f'and calls: {self.__async_remote_calls}')
            await asyncio.gather(*remote_calls)
        return n_remote_calls

    def call_remote_async(self,
                          operator_name: str,
                          function_name: Type | str,
                          key,
                          params: tuple = tuple()):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        self.__async_remote_calls.append((operator_name, function_name, key, params))

    async def call_remote_function_no_response(self,
                                               operator_name: str,
                                               function_name: Type | str, key,
                                               params: tuple = tuple(),
                                               serializer: Serializer = Serializer.MSGPACK):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        partition, payload, operator_host, operator_port = self.__prepare_message_transmission(operator_name,
                                                                                               key,
                                                                                               function_name,
                                                                                               params)

        sender_partition: int = make_key_hashable(self.__key) % len(self.__dns[operator_name].keys())
        # if operator_name == "personsFilter":
        #     logging.warning(f"sender partition: {sender_partition}")
        #     logging.warning(f"self.__key: {self.__key}, self.__dns len: {len(self.__dns[operator_name].keys())}")


        await self.__networking.send_message(operator_host,
                                             operator_port,
                                             {"__COM_TYPE__": 'RUN_FUN_REMOTE',
                                              "__MSG__": payload},
                                             serializer,
                                             sending_name=self.__operator_name,
                                             sending_partition=sender_partition)

    async def call_remote_function_request_response(self,
                                                    operator_name: str,
                                                    function_name:  Type | str,
                                                    key,
                                                    params: tuple = tuple()):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        partition, payload, operator_host, operator_port = self.__prepare_message_transmission(operator_name,
                                                                                               key,
                                                                                               function_name,
                                                                                               params)
        logging.info(f'(SF)  Start {operator_host}:{operator_port} of {operator_name}:{partition}')
        resp = await self.__networking.send_message_request_response(operator_host,
                                                                     operator_port,
                                                                     {"__COM_TYPE__": 'RUN_FUN_RQ_RS_REMOTE',
                                                                      "__MSG__": payload},
                                                                     Serializer.MSGPACK)
        return resp

    def __prepare_message_transmission(self, operator_name: str, key, function_name: str, params: tuple):
        try:
            partition: int = make_key_hashable(key) % len(self.__dns[operator_name].keys())

            payload = {'__RQ_ID__': self.__request_id,
                       '__OP_NAME__': operator_name,
                       '__FUN_NAME__': function_name,
                       '__KEY__': key,
                       '__PARTITION__': partition,
                       '__TIMESTAMP__': self.__timestamp,
                       '__PARAMS__': params}

            operator_host = self.__dns[operator_name][str(partition)][0]
            operator_port = self.__dns[operator_name][str(partition)][1]
        except KeyError:
            logging.error(f"Couldn't find operator: {operator_name} in {self.__dns}")
        else:
            return partition, payload, operator_host, operator_port
