from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .bids_source import bids_source_operator
from .currency_mapper import currency_mapper_operator
from .sink import sink_operator


####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('nexmark_q1', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
