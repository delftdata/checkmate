import os
import random

from universalis.common.operator import StatefulFunction, Operator

source_operator = Operator('source', n_partitions=4)


@source_operator.register
async def read(ctx: StatefulFunction, value: float):
    new_key = random.randint(0, 3)
    await ctx.call_remote_function_no_response(operator_name='count',
                                                function_name='addToCount',
                                                key=new_key,
                                                params=(value, ))


