import logging

from universalis.common.operator import StatefulFunction, Operator
from universalis.common.serialization import Serializer

join_operator = Operator('join', n_partitions=4)


@join_operator.register
async def join(ctx: StatefulFunction,
               item: tuple[int, int] | tuple[int, int, set[tuple[int, int]]]):
    # item: link[timestamp, start, end] | source[timestamp, source_node, reachable_node, binary_vector]
    async with ctx.lock:
        state = await ctx.get()
        # Init the state if it's empty:
        if state is None:
            state = {'links': set(), 'sources': {}}
        if len(item) == 3:
            # source
            source_node = item[0]
            reachable_node = item[1]
            binary_vector = item[2]
            key = (source_node, reachable_node)
            if key in state['sources']:
                if isinstance(state['sources'][key], list):
                    state['sources'][key].append(binary_vector)
                else:
                    state['sources'][key] = [binary_vector]
        else:
            # link
            start_node = item[0]
            end_node = item[1]
            key = (start_node, end_node)
            if key not in state['links']:
                state['links'].add(key)
        await ctx.put(state)

    # logging.warning(f"item received from join: {item}")

    if len(item) == 3:
        # source
        for link in state['links']:
            start_node, end_node = link
            if start_node == reachable_node:
                joined_item = link + item
                await ctx.call_remote_function_no_response(operator_name='select',
                                                           function_name='filter_ones',
                                                           key=ctx.key,
                                                           params=(joined_item, ),
                                                           serializer=Serializer.PICKLE)
    else:
        # link
        for source, binary_vectors in state['sources'].items():
            source_node, reachable_node = source
            if start_node == reachable_node:
                for binary_vector in binary_vectors:
                    joined_item = item + source + (binary_vector, )
                    await ctx.call_remote_function_no_response(operator_name='select',
                                                               function_name='filter_ones',
                                                               key=ctx.key,
                                                               params=(joined_item, ),
                                                               serializer=Serializer.PICKLE)


@join_operator.register
async def remove(ctx: StatefulFunction,
                 item: tuple[int, int] | int):
    async with ctx.lock:
        state = await ctx.get()
        # Init the state if it's empty:
        if state is not None:
            if len(item) == 4:
                # source
                for source_key in state['sources']:
                    source_node, _ = source_key
                    if item == source_node:
                        del state['sources'][source_key]
            else:
                # link
                start_node = item[0]
                end_node = item[1]
                link_to_delete = (start_node, end_node)
                if link_to_delete in state['links']:
                    state['links'].remove(link_to_delete)
                for source_key, binary_vectors in state['sources'].items():
                    for binary_vector in binary_vectors:
                        if link_to_delete in binary_vector:
                            state['sources'][source_key].remove(binary_vector)
            await ctx.put(state)
