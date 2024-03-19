from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .source import source_operator
from .sink import sink_operator
from .count import count_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('consistency-demo', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(source_operator, count_operator, sink_operator)
