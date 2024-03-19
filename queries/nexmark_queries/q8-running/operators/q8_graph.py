from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .auctions_source import auctions_source_operator
from .persons_source import persons_source_operator
from .join_operator import join_operator
from .sink import sink_operator
from .tumbling_window import tumbling_window_operator


####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('nexmark_q8', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
