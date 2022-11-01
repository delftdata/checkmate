import asyncio

import redis.asyncio as redis

from universalis.common.base_state import BaseOperatorState
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization


class RedisOperatorState(BaseOperatorState):

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.redis_connections: dict[str, redis.Redis] = {}
        self.writing_to_db_locks: dict[str, asyncio.Lock] = {}
        for i, operator_name in enumerate(operator_names):
            self.redis_connections[operator_name] = redis.Redis(unix_socket_path='/tmp/redis.sock', db=i)
            self.writing_to_db_locks[operator_name] = asyncio.Lock()

    async def get(self, key, operator_name: str):
        async with self.writing_to_db_locks[operator_name]:
            db_value = await self.redis_connections[operator_name].get(key)
        if db_value is None:
            return None
        else:
            value = msgpack_deserialization(db_value)
            return value

    async def put(self, key, value, operator_name: str):
        async with self.writing_to_db_locks[operator_name]:
            serialized_value = msgpack_serialization(value)
            await self.redis_connections[operator_name].set(key, serialized_value)

    async def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return True if await self.redis_connections[operator_name].exists(key) > 0 else False
