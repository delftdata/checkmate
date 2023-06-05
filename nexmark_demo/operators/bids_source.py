from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging

bids_source_operator = Operator('bids_source', n_partitions=6)

@bids_source_operator.register
async def read(ctx: StatefulFunction, *kwargs):
    return kwargs[1]