from universalis.common.logging import logging

from universalis.common.operator import StatefulFunction, Operator

count_operator = Operator('count', n_partitions=4)
logging_time = 0


@count_operator.register
async def addToCount(ctx: StatefulFunction, value: int):
    async with ctx.lock:
        current_count: set[int] = await ctx.get()
        if current_count is None:
            current_count = {value}
        else:
            if value not in current_count:
                current_count.add(value)
            else:
                logging.warning(f'Value: {value} is duplicate')
        await ctx.put(current_count)
    await ctx.call_remote_function_no_response(operator_name='sink',
                                               function_name='output',
                                               key=ctx.key,
                                               params=(value, ))


@count_operator.register
async def triggerLogging(ctx: StatefulFunction, value: float):
    logging.warning('Running triggerLogging')
    async with ctx.lock:
        current_count = await ctx.get_operator_state()
    logging.warning(current_count)
