import time
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.nexmark.entities import Bid
from universalis.common.serialization import Serializer

count_operator = Operator('count', n_partitions=50)

@count_operator.register
async def count(ctx: StatefulFunction, items: list):
    current_count = len(items)
    await ctx.call_remote_function_no_response(
        operator_name='sink',
        function_name='output',
        key=ctx.key,
        params=(ctx.key, current_count, ),
        serializer=Serializer.PICKLE
    )