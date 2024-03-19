from universalis.common.operator import StatefulFunction, Operator

sink_operator = Operator('sink', n_partitions=4)


@sink_operator.register
async def output(ctx: StatefulFunction, value: tuple[int, int, set[tuple[int, int]]]):
    return value
