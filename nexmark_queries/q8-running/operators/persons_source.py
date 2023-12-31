from universalis.common.operator import StatefulFunction, Operator
from universalis.common.logging import logging
from universalis.common.serialization import Serializer
from universalis.nexmark.entities import Person

persons_source_operator = Operator('personsSource')


@persons_source_operator.register
async def read(ctx: StatefulFunction, *args):
    person = Person(*args)
    await ctx.call_remote_function_no_response(
        operator_name='join',
        function_name='windowed_join',
        key=person.id,
        params=(person,),
        serializer=Serializer.PICKLE
    )