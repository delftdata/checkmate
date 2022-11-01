from abc import ABC, abstractmethod


class Function(ABC):

    def __init__(self):
        self.name = type(self).__name__

    def __call__(self, *args, **kwargs):
        return self.run(*args)

    @abstractmethod
    def run(self, *args):
        raise NotImplementedError
