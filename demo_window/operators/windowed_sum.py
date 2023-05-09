from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging

windowed_sum_operator = Operator('windowed_sum', n_partitions=6)

@windowed_sum_operator.register
async def windowed_sum(ctx: StatefulFunction, timestamp: float , window: list):
    # return (timestamp, ctx.key, sum(window))
    ctx.call_remote_async(
            operator_name='sum_all',
            function_name='sum_all',
            key=1,
            params=(timestamp, 1, sum(window),)
    )
