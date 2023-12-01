from universalis.common.operator import StatefulFunction, Operator
from universalis.common.serialization import Serializer

project_operator = Operator('project', n_partitions=4)


@project_operator.register
async def discard_and_update(ctx: StatefulFunction, item: tuple[int, int, int, int, set[tuple[int, int]]]):
    start_node, end_node, source_node, reachable_node, binary_vector = item
    binary_vector.add((start_node, end_node))
    output_item = (source_node, end_node, binary_vector)
    await ctx.call_remote_function_no_response(operator_name='sink',
                                               function_name='output',
                                               key=ctx.key,
                                               params=(output_item,),
                                               serializer=Serializer.PICKLE
                                               )
    await ctx.call_remote_function_no_response(operator_name='join',
                                               function_name='join',
                                               key=end_node,
                                               params=(output_item,),
                                               serializer=Serializer.PICKLE
                                               )
