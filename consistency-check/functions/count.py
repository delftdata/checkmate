import asyncio
import random
import time
from universalis.common.logging import logging

from universalis.common.operator import StatefulFunction, Operator

count_operator = Operator('count', n_partitions=6)
logging_time = 0


@count_operator.register
async def addToCount(ctx: StatefulFunction, value: float):
    async with ctx.lock:
        current_count = await ctx.get()
        if isinstance(current_count, int):
            current_count += 1
        else:
            current_count = 1
        await ctx.put(current_count)
    await ctx.call_remote_function_no_response(operator_name='sink',
                                                function_name='output',
                                                key=ctx.key,
                                                params=(current_count, ))

@count_operator.register
async def triggerLogging(ctx: StatefulFunction, value: float):
        async with ctx.lock:
            current_count = await ctx.get()
        logging.warning(current_count)
