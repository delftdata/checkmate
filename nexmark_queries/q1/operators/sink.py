from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging

sink_operator = Operator('sink', n_partitions=30)

@sink_operator.register
async def output(ctx: StatefulFunction, *args):
    return args