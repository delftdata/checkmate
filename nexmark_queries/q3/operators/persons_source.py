from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.common.serialization import Serializer
from universalis.nexmark.entities import Person

persons_source_operator = Operator('persons_source', n_partitions=6)

@persons_source_operator.register
async def read(ctx: StatefulFunction, *args):
    person = Person(*args)
    await ctx.call_remote_function_no_response(
        operator_name='persons_filter',
        function_name='filter',
        key=ctx.key,
        params=(person,),
        serializer=Serializer.CLOUDPICKLE
    )