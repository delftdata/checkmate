import asyncio
import subprocess
import time

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.nexmark.config import config
from universalis.universalis import Universalis

from operators.auctions_source import auctions_source_operator
from operators.persons_source import persons_source_operator
from operators.sink import sink_operator
from operators.join_operator import join_operator
from operators import q8_graph

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    args = config()
    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()

    channel_list = [
        (None, 'personsSource', False),
        (None, 'auctionsSource', False),
        ('personsSource', 'join', True),
        ('auctionsSource', 'join', True),
        ('join', 'sink', False),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    scale = int(args.persons_partitions)
    auctions_source_operator.set_partitions(scale)
    persons_source_operator.set_partitions(scale)
    join_operator.set_partitions(scale)
    sink_operator.set_partitions(scale)
    q8_graph.g.add_operators(auctions_source_operator, persons_source_operator, join_operator, sink_operator)
    await universalis.submit(q8_graph.g)

    print('Graph submitted')

    time.sleep(60)
    # input("Press when you want to start producing")

    # START WINDOW TRIGGER
    tasks = []
    for key in range(0, scale):
        tasks.append(universalis.send_kafka_event(operator=join_operator,
                                                  key=key,
                                                  function="trigger",
                                                  params=(10,)
                                                  ))
    print(len(tasks))
    responses = await asyncio.gather(*tasks)
    print(responses)
    tasks = []
    # SEND REQUESTS
    # time.sleep(10)

    subprocess.call(["java", "-jar", "nexmark/target/nexmark-generator-1.0-SNAPSHOT-jar-with-dependencies.jar",
                     "--query", "3",
                     "--generator-parallelism", "1",
                     "--enable-auctions-topic", "true",
                     "--enable-persons-topic", "true",
                     "--load-pattern", "static",
                     "--experiment-length", "1",
                     "--use-default-configuration", "false",
                     "--rate", args.rate,
                     "--max-noise", "0",
                     "--iteration-duration-ms", "90000",
                     "--kafka-server", "localhost:9093",
                     "--uni-persons-partitions", args.persons_partitions,
                     "--uni-auctions-partitions", args.auctions_partitions,
                     "--skew", args.skew
                     ])

    await universalis.close()


uvloop.install()
asyncio.run(main())
