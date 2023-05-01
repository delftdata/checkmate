from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging

sum_all_operator = Operator('sum_all', n_partitions=6)

@sum_all_operator.register
async def sum_all(ctx: StatefulFunction, timestamp: float , key: int, value: float):
    async with ctx.lock:
        global_sum = await ctx.get()
        if isinstance(global_sum, float):
            global_sum += value
        else:
            global_sum = value
        await ctx.put(global_sum)
    return (timestamp, ctx.key, global_sum)
