import random

from universalis.common.operator import StatefulFunction, Operator

first_map_operator = Operator('firstmap', n_partitions=6)

@first_map_operator.register
async def find_neighbors(ctx: StatefulFunction, edge: tuple[str, str, int], key_used: int):
    matching_edges = {}
    async with ctx.lock:
        current_dict = await ctx.get()
        if current_dict is None:
            current_dict = {}
        if edge[1] in current_dict:
            matching_edges = current_dict[edge[1]]
        if edge[2] == 1:
            if edge[0] not in current_dict:
                current_dict[edge[0]] = {}
            current_dict[edge[0]][edge[1]] = edge[2]
        await ctx.put(current_dict)
    result = []
    result.append(edge)
    for edge_end in matching_edges.keys():
        current_edge = (edge[1], edge_end, matching_edges[edge_end])
        result.append(current_edge)
    if len(result) > 1:
        await ctx.call_remote_function_no_response(operator_name='secondmap',
                                                    function_name='nhop_check',
                                                    key=key_used,
                                                    params=(result, key_used, ))