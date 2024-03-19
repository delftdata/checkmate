import asyncio
import random
from universalis.common.logging import logging

from universalis.common.operator import StatefulFunction, Operator
from universalis.nexmark.entities import Auction, Entity, Person

join_operator = Operator('join')


@join_operator.register
async def windowed_join(ctx: StatefulFunction, item: Entity):
    async with ctx.operator_lock:
        state = await ctx.get()
        # Init the state if it's empty:
        if state is None:
            state = {'auctions': [], 'persons': []}

        if isinstance(item, Auction):
            state['auctions'].append(item.to_tuple())
        elif isinstance(item, Person):
            state['persons'].append(item.to_tuple())

        await ctx.put(state)

    if isinstance(item, Auction):
        for person in state['persons']:
            joined_row = (item.to_tuple(), person,)
            await ctx.call_remote_function_no_response(
                operator_name='sink',
                function_name='output',
                key=ctx.key,
                params=(joined_row,)
            )

    elif isinstance(item, Person):
        for auction in state['auctions']:
            joined_row = (item.to_tuple(), auction,)
            await ctx.call_remote_function_no_response(
                operator_name='sink',
                function_name='output',
                key=ctx.key,
                params=(joined_row,)
            )


@join_operator.register
async def trigger(ctx: StatefulFunction, trigger_interval_sec: float):
    await asyncio.sleep(trigger_interval_sec / 2)
    while True:
        async with ctx.operator_lock:
            await ctx.clean_operator_state()
        await asyncio.sleep(trigger_interval_sec)
