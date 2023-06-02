from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.nexmark.entities import Auction
from universalis.common.serialization import Serializer

auctions_source_operator = Operator('auctions_source', n_partitions=6)

@auctions_source_operator.register
async def read(ctx: StatefulFunction, *kwargs):
    auction = Auction(kwargs)
    await ctx.call_remote_function_no_response(
        operator_name='join',
        function_name='statefull_join',
        key=auction.seller,
        params=((auction, 'r', )),
        serializer=Serializer.CLOUDPICKLE
    )