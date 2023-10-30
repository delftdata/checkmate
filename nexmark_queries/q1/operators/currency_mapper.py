from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.nexmark.entities import Bid

currency_mapper_operator = Operator('currencyMapper')

@currency_mapper_operator.register
async def dollarToEuro(ctx: StatefulFunction, bid: Bid):
    exchange_rate = 0.82
    price_in_euro = exchange_rate * bid.price
    await ctx.call_remote_function_no_response(
        operator_name='sink',
        function_name='output',
        key=ctx.key,
        params=((bid.auction, bid.bidder, price_in_euro, bid.dateTime),)
    )