import random

from universalis.common.operator import StatefulFunction, Operator

filter_operator = Operator('filter', n_partitions=6)


@filter_operator.register
async def filter_non_positive(ctx: StatefulFunction, value: float):
    if value > 0:
        await ctx.call_remote_function_no_response(operator_name='map',
                                                   function_name='double_value',
                                                   key=random.randint(0, 6),
                                                   params=(value, ))
