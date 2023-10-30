import random

from universalis.common.operator import StatefulFunction, Operator
from universalis.nexmark.entities import Auction, Entity, Person

join_operator = Operator('join')


@join_operator.register
async def stateless_join(ctx: StatefulFunction, items: list):
    # Currently assuming a list representation looking like the following:
    # [(list, 'l'), (list, 'r'), (list, 'r'), ...]
    # Where the list represent a 'database row'
    left_items = []
    right_items = []
    for item in items:
        if item[1] == 'r':
            right_items.append(item)
        else:
            left_items.append(item)

    joined_result = []
    for lfit in left_items:
        for rgit in right_items:
            joined_result.append((lfit, rgit,))

    return joined_result


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
