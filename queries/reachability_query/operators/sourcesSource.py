from universalis.common.logging import logging
from universalis.common.operator import StatefulFunction, Operator
from universalis.common.serialization import Serializer

sources_operator = Operator('sourcesSource', n_partitions=4)


@sources_operator.register
async def read(ctx: StatefulFunction, source: tuple[int, int, set[tuple[int, int]]]):
    source_node, reachable_node, binary_vector = source
    await ctx.call_remote_function_no_response(operator_name='join',
                                               function_name='join',
                                               key=reachable_node,
                                               params=(source,),
                                               serializer=Serializer.PICKLE)
