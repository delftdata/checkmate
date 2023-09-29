from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.nexmark.entities import Auction
from universalis.common.serialization import Serializer

auctions_source_operator = Operator('auctionsSource', n_partitions=10)

@auctions_source_operator.register
async def read(ctx: StatefulFunction, *args):
    auction = Auction(*args)
    await ctx.call_remote_function_no_response(
        operator_name='tumblingWindow',
        function_name='add',
        key=auction.seller,
        params=(auction,),
        serializer=Serializer.PICKLE
    )