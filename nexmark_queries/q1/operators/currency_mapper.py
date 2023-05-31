from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging

currency_mapper_operator = Operator('currency_mapper', n_partitions=6)

@currency_mapper_operator.register
async def dollar_to_euro(ctx: StatefulFunction, *kwargs):
    exchange_rate = 0.82
    price = kwargs[2]
    price_in_euro = exchange_rate * price
    await ctx.call_remote_function_no_response(
        operator_name='sink',
        function_name='output',
        key=ctx.key,
        params=((kwargs[0], kwargs[1], price_in_euro, kwargs[3]),)
    )