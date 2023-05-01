from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .tumbling_window import tumbling_window_operator
from .windowed_sum import windowed_sum_operator
from .sum_all import sum_all_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('windowed_sum_demo', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(tumbling_window_operator, windowed_sum_operator, sum_all_operator)
