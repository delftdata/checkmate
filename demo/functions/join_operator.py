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
    # Divide the items to their respective dictionaries based on the second tuple element
    for item in items:
        if item[1] == 'r':
            # Immediately map (hash) the items per key that they should be joined on
            # These left or right attribute indexes could be specified as environment variables instead of being passed from the upstream operator
            if item[0][right_attribute_index] not in right_items.keys:
                right_items[item[0][right_attribute_index]] = [item[0]]
            else:
                right_items[item[0][right_attribute_index]].append(item[0])
        else:
            if item[0][left_attribute_index] not in left_items.keys:
                left_items[item[0][left_attribute_index]] = [item[0]]
            else:
                left_items[item[0][left_attribute_index]].append(item[0])


    # Create the joined results
    joined_result = []
    # Look for all the matching keys
    for key in left_items.keys():
        if key in right_items.keys():
            for left_row in left_items[key]:
                for right_row in right_items[key]:
                    # Create a joined output for all possible combinations;
                    # Simple copy all the values from 'left' items and add all values from the 'right' items except that we value that we joined on.
                    joined_row = left_row
                    for index in range(len(right_row)):
                        if index == right_attribute_index:
                            continue
                        else:
                            joined_row.append(right_row[index])
                    joined_result.append(joined_row)

    # Important to note here is that we expect only the values as input, so if we have one 'table' containing e.g. (e-mail, first name, age)
    # When joining this with (first name, gender) input would look like: [(['email@address.com', 'Joe', 15], 'l'), (['Joe', 'male'], 'r')], 1, 0
    # In this case the resulting output would be; [['email@address.com', 'Joe', 15, 'male']]
    # The 1 and the 0 (left and right attribute index) could be specified as environment variables instead
    # The list representation could be replaced with a map representation if prefered; {'e-mail': 'email@address.com', ...}

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
    # Iterate over left items, match with right items and the right items from the state
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

    # Iterate over the right items to match them with the left items in the state.
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

    # Add the new items to the state after the join results have been generated
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
