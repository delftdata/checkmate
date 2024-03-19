import random

from universalis.common.operator import StatefulFunction, Operator
from universalis.common.serialization import Serializer

links_operator = Operator('linksSource', n_partitions=4)


@links_operator.register
async def read(ctx: StatefulFunction, link: tuple[int, int]):
    start_node, end_node = link
    await ctx.call_remote_function_no_response(operator_name='join',
                                               function_name='join',
                                               key=start_node,
                                               params=(link,),
                                               serializer=Serializer.PICKLE)
