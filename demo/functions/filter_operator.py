import random

from universalis.common.operator import StatefulFunction, Operator

filter_operator = Operator('filter', n_partitions=6)


@filter_operator.register
async def filter_non_positive(ctx: StatefulFunction, value: float):
    current_count = await ctx.get()
    if isinstance(current_count, int):
        current_count += 1
    else:
        current_count = 1
    await ctx.put(current_count)
    if value > 0:
        await ctx.call_remote_function_no_response(operator_name='map',
                                                   function_name='double_value',
                                                   key=random.randint(0, 6),
                                                   params=(value, ))

@filter_operator.register
async def filter_cyclic_start(ctx: StatefulFunction, value: float, key_used: int):
    await ctx.call_remote_function_no_response(operator_name='map',
                                                   function_name='map_cyclic_query',
                                                   key=random.randint(0, 6),
                                                   params=(value, key_used, ))

@filter_operator.register
async def filter_cyclic_end(ctx: StatefulFunction, value: float):
    current_count = await ctx.get()
    if isinstance(current_count, int):
        current_count += 1
    else:
        current_count = 1
    await ctx.put(current_count)
    return 0