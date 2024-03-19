from universalis.common.operator import StatefulFunction, Operator
from universalis.common.serialization import Serializer

select_operator = Operator('select', n_partitions=4)


@select_operator.register
async def filter_ones(ctx: StatefulFunction,
                      item: tuple[int, int, int, int, set[tuple[int, int]]]):
    start_node, end_node, source_node, reachable_node, binary_vector = item
    broken = False
    for bv_start_node, bv_end_node in binary_vector:
        if bv_end_node == end_node:
            broken = True
            break
    if not broken:
        await ctx.call_remote_function_no_response(operator_name='project',
                                                   function_name='discard_and_update',
                                                   key=ctx.key,
                                                   params=(item, ),
                                                   serializer=Serializer.PICKLE
                                                   )
