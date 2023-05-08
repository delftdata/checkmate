from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .bids_source import bids_source_operator


####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('nexmark_demo', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(bids_source_operator)
