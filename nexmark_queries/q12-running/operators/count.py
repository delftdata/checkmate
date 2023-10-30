import asyncio
import time
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.serialization import Serializer

count_operator = Operator('count')


@count_operator.register
async def count(ctx: StatefulFunction, _):

    async with ctx.operator_lock:
        current_count = await ctx.get()
        if current_count is None:
            current_count = 1
        else:
            current_count += 1
        await ctx.call_remote_function_no_response(
            operator_name='sink',
            function_name='output',
            key=ctx.key,
            params=(ctx.key, current_count,),
            serializer=Serializer.PICKLE
        )
        await ctx.put(current_count)


@count_operator.register
async def trigger(ctx: StatefulFunction, trigger_interval_sec: float):
    await asyncio.sleep(trigger_interval_sec/2)
    while True:
        async with ctx.operator_lock:
            await ctx.clean_operator_state()
        await asyncio.sleep(trigger_interval_sec)
