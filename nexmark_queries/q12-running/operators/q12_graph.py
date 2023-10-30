from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .bids_source import bids_source_operator
from .count import count_operator
from .sink import sink_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('nexmark_q12', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
