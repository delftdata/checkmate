import random
from universalis.common.logging import logging

from universalis.common.operator import StatefulFunction, Operator
from universalis.nexmark.entities import Auction, Entity, Person

join_operator = Operator('join', n_partitions=50)


@join_operator.register
async def stateless_join(ctx: StatefulFunction, ingested_item: Entity, items: list):
    for item in items:
        joined_row = (ingested_item.to_tuple(), item.to_tuple(), )
        await ctx.call_remote_function_no_response(
            operator_name='sink',
            function_name='output',
            key=ctx.key,
            params=(joined_row,)
        )


@join_operator.register
async def stateful_join(ctx: StatefulFunction, item: Entity):
    async with ctx.lock:
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
        for person in state['persons'][:-1]:
                joined_row = (item.to_tuple(), person,)
                await ctx.call_remote_function_no_response(
                    operator_name='sink',
                    function_name='output',
                    key=ctx.key,
                    params=(joined_row,)
                )

    elif isinstance(item, Person):
        for auction in state['auctions'][:-1]:
                joined_row = (item.to_tuple(), auction, )
                await ctx.call_remote_function_no_response(
                    operator_name='sink',
                    function_name='output',
                    key=ctx.key,
                    params=(joined_row,)
                )

