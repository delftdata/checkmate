import time
import asyncio
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.common.serialization import Serializer
from universalis.nexmark.entities import Entity


tumbling_window_operator = Operator('tumblingWindow', n_partitions=6)

@tumbling_window_operator.register
async def add(ctx: StatefulFunction, item: Entity):
    async with ctx.operator_lock:
        current_window = await ctx.get()
        if isinstance(current_window, list):
            current_window.append(item)
        else:
            current_window = [item]
        await ctx.put(current_window)
    await ctx.call_remote_function_no_response(
        operator_name='count',
        function_name='stateless_join',
        key=ctx.key,
        params=(time.time(), current_window, ),
        serializer=Serializer.PICKLE
    )


@tumbling_window_operator.register
async def trigger(ctx: StatefulFunction, trigger_interval_sec: float):
    while True:
        await asyncio.sleep(trigger_interval_sec)
        async with ctx.operator_lock:
            await ctx.clean_operator_state()
        