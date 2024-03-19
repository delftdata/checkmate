import asyncio
import random
import subprocess
import time

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.nexmark.config import config
from universalis.universalis import Universalis

from operators.bids_source import bids_source_operator
from operators.currency_mapper import currency_mapper_operator
from operators.sink import sink_operator
from operators.q1_graph import g

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
        (None, 'bidsSource', False),
        ('bidsSource', 'currencyMapper', False),
        ('currencyMapper', 'sink', False),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    await asyncio.sleep(5)

    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    scale = int(args.bids_partitions)
    bids_source_operator.set_partitions(scale)
    currency_mapper_operator.set_partitions(scale)
    sink_operator.set_partitions(scale)
    g.add_operators(bids_source_operator, currency_mapper_operator, sink_operator)
    await universalis.submit(g)

    print('Graph submitted')

    time.sleep(60)
    # input("Press when you want to start producing.")

    subprocess.call(["java", "-jar", "nexmark/target/nexmark-generator-1.0-SNAPSHOT-jar-with-dependencies.jar",
                     "--generator-parallelism", "1",
                     "--enable-bids-topic", "true",
                     "--load-pattern", "static",
                     "--experiment-length", "1",
                     "--use-default-configuration", "false",
                     "--rate", args.rate,
                     "--max-noise", "0",
                     "--iteration-duration-ms", "90000",
                     "--kafka-server", "localhost:9093",
                     "--uni-bids-partitions", args.bids_partitions
                     ])

    await universalis.close()


uvloop.install()
asyncio.run(main())
