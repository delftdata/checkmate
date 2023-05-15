from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .first_map import first_map_operator
from .second_map import second_map_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('nhop_approximation', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(first_map_operator, second_map_operator)
