import time
import asyncio
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging


tumbling_window_operator = Operator('tumbling_window', n_partitions=6)

@tumbling_window_operator.register
async def window(ctx: StatefulFunction, value: float):
    async with ctx.lock:
        current_window = await ctx.get()
        logging.warning(f"key: {ctx.key}")
        if isinstance(current_window, list):
            current_window.append(value)
        else:
            current_window = [value]
        logging.warning(f"key, window_legth: {ctx.key}, {len(current_window)}")
        await ctx.put(current_window)


@tumbling_window_operator.register
async def trigger(ctx: StatefulFunction, trigger_interval_sec: float):
    while True:
        await asyncio.sleep(trigger_interval_sec)
        async with ctx.lock:
            current_window = await ctx.get()
            if current_window is None:
                current_window = []
            logging.warning(f"key {ctx.key} trigger")
            await ctx.call_remote_function_no_response(
                operator_name='windowed_sum',
                function_name='windowed_sum',
                key=ctx.key,
                params=(time.time() ,current_window, )
            )
            await ctx.put([])
        