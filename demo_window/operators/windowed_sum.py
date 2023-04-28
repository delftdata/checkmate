from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging

windowed_sum_operator = Operator('windowed_sum', n_partitions=6)

@windowed_sum_operator.register
async def sum(ctx: StatefulFunction, timestamp: float , window: list):
    sum = 0
    logging.warning(window)
    for item in window:
        sum+=item
    return (timestamp, sum)