from universalis.common.operator import StatefulFunction, Operator

map_operator = Operator('map', n_partitions=6)


@map_operator.register
async def double_value(ctx: StatefulFunction, value: float):
    return value * 2
