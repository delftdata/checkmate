import time
import asyncio
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.common.serialization import Serializer
from universalis.nexmark.entities import Auction, Entity


tumbling_window_operator = Operator('tumblingWindow', n_partitions=6)

@tumbling_window_operator.register
async def add(ctx: StatefulFunction, item: Entity):
    async with ctx.operator_lock:
        current_window = await ctx.get()
        if ~isinstance(current_window, dict):
            current_window = {"auctions": [], "persons": []}
        else:
            if isinstance(item, Auction):
                current_window["auctions"].append(item)
            else:
                current_window["persons"].append(item)
        await ctx.put(current_window)
    if isinstance(item, Auction):
        await ctx.call_remote_function_no_response(
            operator_name='join',
            function_name='stateless_join',
            key=ctx.key,
            params=(current_window["persons"], ),
            serializer=Serializer.CLOUDPICKLE
        )
    else:
        await ctx.call_remote_function_no_response(
            operator_name='join',
            function_name='stateless_join',
            key=ctx.key,
            params=(current_window["auctions"], ),
            serializer=Serializer.CLOUDPICKLE
        )
        



@tumbling_window_operator.register
async def trigger(ctx: StatefulFunction, trigger_interval_sec: float):
    while True:
        await asyncio.sleep(trigger_interval_sec)
        async with ctx.operator_lock:
            await ctx.clean_operator_state()
        