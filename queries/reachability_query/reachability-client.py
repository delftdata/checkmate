import asyncio
import random
import time
from multiprocessing import Process
from timeit import default_timer as timer
import argparse


import uvloop
from universalis.common.serialization import Serializer
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from operators.graph import g
from operators.sourcesSource import sources_operator
from operators.linksSource import links_operator
from operators.sink import sink_operator
from operators.join import join_operator
from operators.project import project_operator
from operators.select import select_operator

N_NODES = 1_000_000
SECONDS_TO_RUN = 90
N_THREADS = 2
# messages_per_second = 2000
sleeps_per_second = 100
sleep_time = 0.00085

# N_PARTITIONS = 4
add_source_portion = 0.15
delete_source_portion = 0.05
add_link_portion = 0.60
delete_link_portion = 0.20

assert add_source_portion + delete_source_portion + add_link_portion + delete_link_portion == 1

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'

sources_mem: list[int] = []
links_mem: list[tuple[int, int]] = []


def config():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-r", "--rate",
                            help="Provide the input rate.",
                            default="2000",
                            type=str,
                            action="store")
    arg_parser.add_argument("-p", "--partitions",
                            help="Provide the number of partitions.",
                            default="4",
                            type=str,
                            action="store")
    arguments = arg_parser.parse_args()

    return arguments


def generate_event(scale):
    coin = random.random()
    if coin <= add_source_portion:
        # add source
        node_to_become_source = random.randint(1, N_NODES)
        sources_mem.append(node_to_become_source)
        params: tuple[int, int, set[tuple[int, int]]] = (node_to_become_source,
                                                         node_to_become_source,
                                                         {(node_to_become_source, node_to_become_source)})
        partition = random.randint(0, scale - 1)
        return sources_operator, partition, 'read', params
    elif add_source_portion < coin <= add_source_portion + delete_source_portion:
        # del source
        if sources_mem:
            source_to_del = sources_mem.pop(random.randint(0, len(sources_mem) - 1))
            return join_operator, None, 'remove', source_to_del
        return generate_event(scale)
    elif (add_source_portion + delete_source_portion < coin
          <= add_source_portion + delete_source_portion + add_link_portion):
        # add link
        second_coin = random.random()
        if second_coin <= 0.1 and sources_mem:
            start_node = random.choice(sources_mem)
        elif second_coin <= 0.3 and links_mem:
            _, existing_end = random.choice(links_mem)
            start_node = existing_end
        else:
            start_node = random.randint(1, N_NODES)
        end_node = random.randint(1, N_NODES)

        while start_node == end_node:
            end_node = random.randint(1, N_NODES)
        params: tuple[int, int] = (start_node, end_node)
        links_mem.append(params)
        partition = random.randint(0, scale - 1)
        return links_operator, partition, 'read', params
    else:
        # del link
        if links_mem:
            link_to_del = links_mem.pop(random.randint(0, len(links_mem) - 1))
            return join_operator, None, 'remove', link_to_del
        return generate_event(scale)


async def run_generator(do_init):
    args = config()
    messages_per_second = int(args.rate)
    scale = int(args.partitions)

    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()
    if do_init:
        # List of channels; (fromOperator, toOperator, boolean)
        # Use None as fromOperator for source operators
        # Use None as toOperator for sink operators
        # The boolean does not matter in these two cases
        channel_list = [
            (None, 'linksSource', False),
            (None, 'sourcesSource', False),
            (None, 'join', False),
            ('linksSource', 'join', True),
            ('sourcesSource', 'join', True),
            ('join', 'select', False),
            ('select', 'project', False),
            ('project', 'join', True),
            ('project', 'sink', False),
            ('sink', None, False)
        ]

        await universalis.send_channel_list(channel_list)

        ################################################################################################################
        # SUBMIT STATEFLOW GRAPH #######################################################################################
        ################################################################################################################
        sources_operator.set_partitions(scale)
        links_operator.set_partitions(scale)
        join_operator.set_partitions(scale)
        select_operator.set_partitions(scale)
        project_operator.set_partitions(scale)
        sink_operator.set_partitions(scale)
        g.add_operators(sources_operator,
                        links_operator,
                        join_operator,
                        sink_operator,
                        project_operator,
                        select_operator)
        await universalis.submit(g)

        print('Graph submitted')

    time.sleep(5)

    tasks = []
    # SEND REQUESTS
    for _ in range(SECONDS_TO_RUN):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                await asyncio.gather(*tasks)
                tasks = []
                time.sleep(sleep_time)
            operator, key, function_name, params = generate_event(scale)
            if function_name != 'remove':
                tasks.append(universalis.send_kafka_event(operator=operator,
                                                          key=key,
                                                          function=function_name,
                                                          params=(params, ),
                                                          serializer=Serializer.PICKLE))
            else:
                tasks.append(universalis.broadcast_kafka_event(operator=operator,
                                                               function=function_name,
                                                               n_partitions=scale,
                                                               params=(params, ),
                                                               serializer=Serializer.PICKLE))
        await asyncio.gather(*tasks)
        tasks = []
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')

    await universalis.close()

    # print(timestamped_request_ids)
    # pd.DataFrame(timestamped_request_ids, columns=['request_id',
    #                                                'timestamp']).to_csv('client_requests.csv',
    #                                                                     index=False)
    time.sleep(20)


def run_async(do_init: bool):
    uvloop.run(run_generator(do_init))


def main():
    proc_list = []
    for i in range(N_THREADS):
        do_init: bool = i == 0
        p = Process(target=run_async, args=(do_init, ))
        proc_list.append(p)
        p.start()
    for p in proc_list:
        p.join()


if __name__ == "__main__":
    main()
