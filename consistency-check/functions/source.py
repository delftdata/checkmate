import random

from universalis.common.operator import StatefulFunction, Operator

source_operator = Operator('source', n_partitions=6)


@source_operator.register
async def read(ctx: StatefulFunction, value: float):
    await ctx.call_remote_function_no_response(operator_name='count',
                                                function_name='addToCount',
                                                key=ctx.key,
                                                params=(value, ))


