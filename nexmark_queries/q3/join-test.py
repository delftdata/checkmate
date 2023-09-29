import asyncio
import random
import subprocess
import time
import pandas as pd
from timeit import default_timer as timer

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis
from universalis.common.logging import logging

from operators import q3_graph

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await universalis.submit(q3_graph.g)

    print('Graph submitted')

    channel_list = [
        (None, 'personsSource', False),
        (None, 'auctionsSource', False),
        ('personsSource', 'personsFilter', False),
        ('auctionsSource', 'join', False),
        ('personsFilter', 'join', False),
        ('join', 'sink', False),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    time.sleep(60)

    with open("results/q3/test-input.csv", "r") as fp:
        fp.readline