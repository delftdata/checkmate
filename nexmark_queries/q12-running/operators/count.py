import time
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.nexmark.entities import Bid
from universalis.common.serialization import Serializer

count_operator = Operator('count', n_partitions=6)

@count_operator.register
async def count(ctx: StatefulFunction, timestamp, items: list):
    current_count = len(items)
    await ctx.call_remote_function_no_response(
        operator_name='sink',
        function_name='output',
        key=ctx.key,
        params=(timestamp ,ctx.key, current_count, ),
        serializer=Serializer.CLOUDPICKLE
    )