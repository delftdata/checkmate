from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.nexmark.entities import Bid
from universalis.common.serialization import Serializer

bids_source_operator = Operator('bidsSource')

@bids_source_operator.register
async def read(ctx: StatefulFunction, *args):
    bid = Bid(*args)
    await ctx.call_remote_function_no_response(
        operator_name='currencyMapper',
        function_name='dollarToEuro',
        key=ctx.key,
        params=(bid,),
        serializer=Serializer.PICKLE
    )