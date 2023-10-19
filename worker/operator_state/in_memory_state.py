import asyncio

from typing import Any

from universalis.common.base_state import BaseOperatorState


class InMemoryOperatorState(BaseOperatorState):

    data: dict[str, dict[Any, Any]]

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.data = {}
        self.locks = {}
        self.operator_locks = {}
        for operator_name in operator_names:
            self.data[operator_name] = {}
            self.locks[operator_name] = {}
            self.operator_locks[operator_name] = asyncio.Lock()

    async def get_operator_state(self, operator_name: str):
        return self.data[operator_name]

    async def clean_operator_state(self, operator_name: str):
        self.data[operator_name] = {}

    def get_lock(self, key, operator_name: str) -> asyncio.Lock:
        if key not in self.locks[operator_name]:
            self.locks[operator_name][key] = asyncio.Lock()
        return self.locks[operator_name][key]

    def get_operator_lock(self, key, operator_name: str) -> asyncio.Lock:
        return self.operator_locks[operator_name]

    async def put(self, key, value, operator_name: str):
        self.data[operator_name][key] = value

    async def get(self, key, operator_name: str):
        return self.data[operator_name].get(key)

    async def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return True if key in self.data[operator_name] else False
