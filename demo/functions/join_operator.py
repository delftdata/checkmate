import random

from universalis.common.operator import StatefulFunction, Operator

join_operator = Operator('join', n_partitions=6)


@join_operator.register
async def stateless_join(ctx: StatefulFunction, items: list, left_attribute_index: int, right_attribute_index: int):
    # Currently assuming a list representation looking like the following:
    # [(list, 'l'), (list, 'r'), (list, 'r'), ...]
    # Where the list represent a 'database row'
    left_items = {}
    right_items = {}
    for item in items:
        if item[1] == 'r':
            if item[0][right_attribute_index] not in right_items.keys:
                right_items[item[0][right_attribute_index]] = [item[0]]
            else:
                right_items[item[0][right_attribute_index]].append(item[0])
        else:
            if item[0][left_attribute_index] not in left_items.keys:
                left_items[item[0][left_attribute_index]] = [item[0]]
            else:
                left_items[item[0][left_attribute_index]].append(item[0])

    joined_result = []
    for key in left_items.keys():
        if key in right_items.keys():
            for left_row in left_items[key]:
                for right_row in right_items[key]:
                    joined_row = left_row
                    for index in range(len(right_row)):
                        if index == right_attribute_index:
                            continue
                        else:
                            joined_row.append(right_row[index])
                    joined_result.append(joined_row)

    return joined_result

@join_operator.register
async def stateful_join(ctx: StatefulFunction, items: list, left_attribute_index: int, right_attribute_index: int):
    state = await ctx.get()
    # Init the state if it's empty:
    if 'left_hash' not in state.keys():
        state['left_hash'] = {}
    if 'right_hash' not in state.keys():
        state['right_hash'] = {}

    # This join works similar to the stateless join, with some additional staps
    # We now need to also match the 'new' left with the right in state and the new 'right' with the left in state
    # Afterwards we need to add the new right and left to the state.
    left_items = {}
    right_items = {}
    for item in items:
        if item[1] == 'r':
            if item[0][right_attribute_index] not in right_items.keys:
                right_items[item[0][right_attribute_index]] = [item[0]]
            else:
                right_items[item[0][right_attribute_index]].append(item[0])
        else:
            if item[0][left_attribute_index] not in left_items.keys:
                left_items[item[0][left_attribute_index]] = [item[0]]
            else:
                left_items[item[0][left_attribute_index]].append(item[0])

    joined_result = []
    for key in left_items.keys():
        if key in right_items.keys():
            for left_row in left_items[key]:
                for right_row in right_items[key]:
                    joined_row = left_row
                    for index in range(len(right_row)):
                        if index == right_attribute_index:
                            continue
                        else:
                            joined_row.append(right_row[index])
                    joined_result.append(joined_row)
        if key in state['right_hash'].keys():
            for left_row in left_items[key]:
                for right_row in state['right_hash'][key]:
                    joined_row = left_row
                    for index in range(len(right_row)):
                        if index == right_attribute_index:
                            continue
                        else:
                            joined_row.append(right_row[index])
                    joined_result.append(joined_row)

    for key in right_items.keys():
        if key in state['left_hash'].keys():
            for left_row in state['left_hash'][key]:
                for right_row in right_items[key]:
                    joined_row = left_row
                    for index in range(len(right_row)):
                        if index == right_attribute_index:
                            continue
                        else:
                            joined_row.append(right_row[index])
                    joined_result.append(joined_row)

    for key in left_items.keys():
        if key not in state['left_hash'].keys():
            state['left_hash'][key] = left_items[key]
        else:
            state['left_hash'][key] = state['left_hash'][key] + left_items[key]

    for key in right_items.keys():
        if key not in state['right_hash'].keys():
            state['right_hash'][key] = right_items[key]
        else:
            state['right_hash'][key] = state['right_hash'][key] + right_items[key]

    ctx.put(state)

    return joined_result
