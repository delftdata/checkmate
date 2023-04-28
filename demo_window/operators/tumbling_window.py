import time
import asyncio
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging


tumbling_window_operator = Operator('tumbling_window', n_partitions=6)

@tumbling_window_operator.register
async def window(ctx: StatefulFunction, value: float):
    current_window = await ctx.get()
    if isinstance(current_window, list):
        current_window.append(value)
    else:
        current_window = [value]
    await ctx.put(current_window)


@tumbling_window_operator.register
async def trigger(ctx: StatefulFunction, trigger_interval_sec: float):
    while True:
        await asyncio.sleep(trigger_interval_sec)
        current_window = await ctx.get()
        logging.warning(current_window)
        await ctx.call_remote_function_no_response(
            operator_name='windowed_sum',
            function_name='sum',
            key=ctx.key,
            params=(time.time() ,current_window, )
        )
        await ctx.put([])
