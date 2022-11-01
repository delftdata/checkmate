import unittest

from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.networking import NetworkingManager
from universalis.common.operator import Operator
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateful_function import StatefulFunction

from worker.operator_state.in_memory_state import InMemoryOperatorState


class TestState(unittest.IsolatedAsyncioTestCase):

    async def test_graph_creation(self):
        graph = StateflowGraph('test_graph', operator_state_backend=LocalStateBackend.DICT)
        operator_1 = Operator('operator_1', n_partitions=3)
        operator_2 = Operator('operator_2', n_partitions=2)
        graph.add_operators(operator_1, operator_2)
        assert len(graph.nodes) == 2

        networking = NetworkingManager()
        local_state = InMemoryOperatorState({operator_name for operator_name in graph.nodes.keys()})
        [operator.attach_state_networking(local_state, networking, {}) for
         operator in graph.nodes.values()]

        # add function that uses context
        @operator_1.register
        async def fun_1(ctx: StatefulFunction, other_str: str, some_other_str: str):
            await ctx.put('test_value_')
            value = await ctx.get()
            value = value + other_str + some_other_str
            return value

        args = ('other_str', 'some_other_str')
        res = await operator_1.run_function(1, b'0', 1, 'fun_1', args)
        assert res == 'test_value_other_strsome_other_str'
