import random

from universalis.common.operator import StatefulFunction, Operator

map_operator = Operator('map', n_partitions=6)


@map_operator.register
async def double_value(ctx: StatefulFunction, value: float):
    return value * 2

@map_operator.register
async def map_cyclic_query(ctx: StatefulFunction, value: float, key_used: int):
    create_cycle = random.randint(0, 500)
    if create_cycle == 1:
        await ctx.call_remote_function_no_response(operator_name='filter',
                                                    function_name='filter_cyclic_end',
                                                    key=key_used,
                                                    params=(value, ))
    else:
        return 0