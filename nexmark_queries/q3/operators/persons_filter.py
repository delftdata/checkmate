from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.nexmark.entities import Person
from universalis.common.serialization import Serializer

persons_filter_operator = Operator('persons_filter', n_partitions=6)

@persons_filter_operator.register
async def filter(ctx: StatefulFunction, person: Person):
    if person.state in ["OR", "ID", "CA"]:
        await ctx.call_remote_function_no_response(
            operator_name='join',
            function_name='stateful_join',
            key=ctx.key,
            params=(person,),
            serializer=Serializer.CLOUDPICKLE
        )