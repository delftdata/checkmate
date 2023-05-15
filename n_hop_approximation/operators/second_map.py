import random

from universalis.common.operator import StatefulFunction, Operator

NVALUE = 2

second_map_operator = Operator('secondmap', n_partitions=6)

@second_map_operator.register
async def nhop_check(ctx: StatefulFunction, edges: list[tuple[str, str, int]], key_used: int):
    first_edge = edges[0]
    other_edges = edges[1:]
    new_routes = []
    for edge in other_edges:
        new_route = (first_edge[0], edge[1], first_edge[2] + edge[2])
        new_routes.append(new_route)

    async with ctx.lock:
        current_routes = await ctx.get()
        if current_routes is None:
            current_routes = {}
        for edge in edges:
            if edge[0] not in current_routes:
                current_routes[edge[0]] = {}
            if edge[1] in current_routes[edge[0]]:
                if edge[2] < current_routes[edge[0]][edge[1]]:
                    current_routes[edge[0]][edge[1]] = edge[2]
            else:
                current_routes[edge[0]][edge[1]] = edge[2]
        for route in new_routes:
            if route[0] not in current_routes:
                current_routes[route[0]] = {}
            if route[1] in current_routes[route[0]]:
                if route[2] < current_routes[route[0]][route[1]]:
                    current_routes[route[0]][route[1]] = route[2]
            else:
                current_routes[route[0]][route[1]] = route[2]
        await ctx.put(current_routes)

    results = []

    for route in new_routes:
        if route[2] == NVALUE:
            results.append((route[0], route[1]))
        elif route[2] < NVALUE:
            await ctx.call_remote_function_no_response(operator_name='firstmap',
                                                        function_name='find_neighbors',
                                                        key=key_used,
                                                        params=(route, ))
    
    return results
