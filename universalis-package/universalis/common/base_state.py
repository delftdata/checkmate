from abc import abstractmethod, ABC
import asyncio


class BaseOperatorState(ABC):

    def __init__(self, operator_names: set[str]):
        self.operator_names = operator_names
        self.snapshot_state_lock: asyncio.Lock = asyncio.Lock()
        # snapshot event
        self.snapshot_event: asyncio.Event = asyncio.Event()
        self.snapshot_event.set()
        self.no_failure_event = asyncio.Event()
        self.no_failure_event.set()
    
    @abstractmethod
    async def get_operator_state(self, operator_name: str):
        raise NotImplementedError
    
    @abstractmethod
    async def clean_operator_state(self, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    def get_lock(self, key, operator_name: str):
        raise NotImplementedError
    
    @abstractmethod
    def get_operator_lock(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def put(self, key, value, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def get(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key, operator_name: str):
        raise NotImplementedError
