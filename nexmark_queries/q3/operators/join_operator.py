import random

from universalis.common.operator import StatefulFunction, Operator
from universalis.nexmark.entities import Auction, Person

join_operator = Operator('join', n_partitions=6)


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
            joined_result.append((lfit, rgit, ))

    return joined_result

@join_operator.register
async def stateful_join(ctx: StatefulFunction, item: tuple):
    state = None
    async with ctx.lock:
        state = await ctx.get()
        # Init the state if it's empty:
        if 'left_hash' not in state.keys():
            state['left_hash'] = []
        if 'right_hash' not in state.keys():
            state['right_hash'] = []

        if item[1] == 'l':
            state['left_hash'].append(item[0].to_tuple())
        elif item[1] == 'r':
            state['right_hash'].append(item[0].to_tuple())

        ctx.put(state)

    joined_result = []

    if item[1] == 'l':
        for right_item in state['right_hash'][:-1]:
                joined_row = (item[0].to_tuple(), )
                joined_row.append(right_item)
                joined_result.append(joined_row)
                await ctx.call_remote_function_no_response(
                    operator_name='sink',
                    function_name='output',
                    key=ctx.key,
                    params=(joined_result)
                )

    elif item[1] == 'r':
        for left_item in state['left_hash'][:-1]:
                joined_row = (item[0].to_tuple(), )
                joined_row.append(left_item)
                await ctx.call_remote_function_no_response(
                    operator_name='sink',
                    function_name='output',
                    key=ctx.key,
                    params=(joined_result)
                )

