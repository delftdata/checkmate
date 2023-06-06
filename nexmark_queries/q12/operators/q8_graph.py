from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .bids_source import bids_source_operator
from .count import count_operator
from .sink import sink_operator
from .tumbling_window import tumbling_window_operator


####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('nexmark_q8', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(bids_source_operator, tumbling_window_operator, count_operator, sink_operator)
