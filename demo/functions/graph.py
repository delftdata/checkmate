from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from demo.functions.filter_operator import filter_operator
from demo.functions.map_operator import map_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('filter_and_map_demo', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(filter_operator, map_operator)
