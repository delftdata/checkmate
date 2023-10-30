import logging

from .serialization import msgpack_deserialization
from .base_operator import BaseOperator
from .stateful_function import StatefulFunction

SERVER_PORT = 8888


class NotAFunctionError(Exception):
    pass


class Operator(BaseOperator):

    def __init__(self,
                 name: str,
                 n_partitions: int = 1):
        super().__init__(name, n_partitions)
        self.__state = None
        self.__networking = None
        # where the other functions exist
        self.__dns: dict[str, dict[str, tuple[str, int]]] = {}
        self.__functions: dict[str, type] = {}

    def set_partitions(self, partitions):
        self.n_partitions = partitions

    @property
    def functions(self):
        return self.__functions

    async def run_function(self,
                           key,
                           request_id: bytes,
                           timestamp: int,
                           function_name: str,
                           params: tuple,
                           partition: int) -> tuple[any, bool]:
        logging.info(f'RQ_ID: {msgpack_deserialization(request_id)} function: {function_name}')
        f = self.__materialize_function(function_name, key, request_id, timestamp, partition)
        params = (f, ) + params
        resp = await f(*params)
        del f
        return resp

    def __materialize_function(self, function_name, key, request_id, timestamp, partition):
        f = StatefulFunction(key,
                             self.name,
                             self.__state,
                             self.__networking,
                             timestamp,
                             self.__dns,
                             request_id,
                             partition)
        f.run = self.__functions[function_name]
        return f

    def register(self, func: type):
        self.__functions[func.__name__] = func

    def attach_state_networking(self, state, networking, dns):
        self.__state = state
        self.__networking = networking
        self.__dns = dns
