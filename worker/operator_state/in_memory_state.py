from typing import Any

from universalis.common.base_state import BaseOperatorState


class InMemoryOperatorState(BaseOperatorState):

    data: dict[str, dict[Any, Any]]

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.data = {}
        for operator_name in operator_names:
            self.data[operator_name] = {}

    async def put(self, key, value, operator_name: str):
        self.data[operator_name][key] = value

    async def get(self, key, operator_name: str):
        return self.data[operator_name].get(key)

    async def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return True if key in self.data[operator_name] else False
