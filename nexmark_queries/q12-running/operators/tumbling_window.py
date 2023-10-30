import time
import asyncio
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.common.serialization import Serializer
from universalis.nexmark.entities import Entity


tumbling_window_operator = Operator('tumblingWindow')

@tumbling_window_operator.register
async def add(ctx: StatefulFunction, item: Entity):
    async with ctx.operator_lock:
        current_window = await ctx.get()
        if isinstance(current_window, list):
            current_window.append(item)
        else:
            current_window = [item]
        await ctx.call_remote_function_no_response(
            operator_name='count',
            function_name='count',
            key=ctx.key,
            params=(current_window, ),
            serializer=Serializer.PICKLE
        ) 
        await ctx.put(current_window)


@tumbling_window_operator.register
async def trigger(ctx: StatefulFunction, trigger_interval_sec: float):
    await asyncio.sleep(trigger_interval_sec/2)
    while True:
        async with ctx.operator_lock:
            await ctx.clean_operator_state()
        await asyncio.sleep(trigger_interval_sec)
