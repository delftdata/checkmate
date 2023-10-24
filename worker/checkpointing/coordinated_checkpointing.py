from universalis.common.logging import logging
from aiokafka import TopicPartition


class CoordinatedCheckpointing(object):
    def __init__(self):
        self.id = -1
        self.peers = {}

        self.last_kafka_consumed = {}
        # Used to start the coordinated checkpointing
        self.source_operators = set()
        # Used to forward the checkpointing algorithm (operator > (id, operator))
        self.outgoing_channels = {}
        # Used to check whether all markers are received (operator > (id, operator) > boolean)
        self.incoming_channels = {}
        # Used to check whether the coordinator round is done
        self.sink_operators = {}

    def set_id(self, _id):
        self.id = _id

    def set_peers(self, peers):
        self.peers = peers

    def get_worker_id(self, host, port):
        worker_id = self.id
        for _id in self.peers.keys():
            if (host, port) == self.peers[_id]:
                worker_id = _id
                break
        return worker_id

    def get_source_operators(self):
        return self.source_operators

    def get_outgoing_channels(self, operator):
        if operator in self.outgoing_channels.keys():
            return self.outgoing_channels[operator]
        return set()

    def get_offsets(self):
        return self.last_kafka_consumed

    def process_channel_list(self, channel_list, worker_operators, partitions_to_ids):
        partitions_per_operator = {}
        for op, part in worker_operators:
            if op.name not in partitions_per_operator:
                partitions_per_operator[op.name] = set()
            partitions_per_operator[op.name].add(part)
        for from_op, to_op, broadcast in channel_list:
            if from_op is None:
                self.source_operators.add(to_op)
            elif to_op is None:
                self.sink_operators[from_op] = False
            else:
                if from_op not in self.outgoing_channels.keys():
                    self.outgoing_channels[from_op] = set()
                if to_op not in self.incoming_channels.keys():
                    self.incoming_channels[to_op] = {}
                if broadcast:
                    for _id in self.peers.keys():
                        self.outgoing_channels[from_op].add((_id, to_op))
                        self.incoming_channels[to_op][(_id, from_op)] = False
                else:
                    channel_ids = set()
                    # First set all correct outgoing channels:
                    for part in partitions_per_operator[from_op]:
                        channel_ids.add(partitions_to_ids[to_op][str(part)])
                    for _id in channel_ids:
                        self.outgoing_channels[from_op].add((_id, to_op))
                    channel_ids = set()
                    # Then set all correct incoming channels
                    for part in partitions_per_operator[to_op]:
                        channel_ids.add(partitions_to_ids[from_op][str(part)])
                    for _id in channel_ids:
                        self.incoming_channels[to_op][(_id, from_op)] = False

    def marker_received(self, message):
        sender_id, sender_operator, own_operator, _ = message
        if (own_operator in self.incoming_channels and
                (sender_id, sender_operator) in self.incoming_channels[own_operator]):
            self.incoming_channels[own_operator][(sender_id, sender_operator)] = True
            all_markers_received = True
            for channel in self.incoming_channels[own_operator].keys():
                all_markers_received = all_markers_received and self.incoming_channels[own_operator][channel]
            if all_markers_received:
                return True
            else:
                return False
        else:
            logging.warning('received marker from non-incoming channel.')
            return False

    def unblock_channels(self, operator_name):
        for channel in self.incoming_channels[operator_name].keys():
            self.incoming_channels[operator_name][channel] = False

    def check_marker_received(self, own_operator, sender_id, sender_op):
        if own_operator in self.incoming_channels and (sender_id, sender_op) in self.incoming_channels[own_operator]:
            return self.incoming_channels[own_operator][(sender_id, sender_op)]
        return False

    def set_sink_operator(self, operator):
        # logging.warning(f'sink operators: {self.sink_operators}')
        if operator in self.sink_operators.keys():
            self.sink_operators[operator] = True
        all_sink_operators_checkpointed = True
        for op in self.sink_operators.keys():
            all_sink_operators_checkpointed = all_sink_operators_checkpointed and self.sink_operators[op]
        if all_sink_operators_checkpointed:
            for op in self.sink_operators.keys():
                self.sink_operators[op] = False
            return True
        else:
            return False

    def get_partitions_to_reset(self, operator):
        if operator not in self.last_kafka_consumed.keys():
            return []
        tp_to_reset = []
        for partition in self.last_kafka_consumed[operator].keys():
            tp_to_reset.append(TopicPartition(operator, int(partition)))
        return tp_to_reset

    def find_kafka_to_replay(self, operator_name, last_kafka_consumed):
        to_replay = []
        if operator_name in self.last_kafka_consumed.keys():
            for partition in self.last_kafka_consumed[operator_name]:
                if partition in last_kafka_consumed:
                    to_replay.append((TopicPartition(operator_name, int(partition)),
                                      last_kafka_consumed[partition] + 1))
                else:
                    last_kafka_consumed[partition] = 0
                    to_replay.append((TopicPartition(operator_name, int(partition)), 0))
            self.last_kafka_consumed[operator_name] = last_kafka_consumed
        return to_replay

    def set_consumed_offset(self, topic, partition, offset):
        if topic not in self.last_kafka_consumed:
            self.last_kafka_consumed[topic] = {}
        str_part = str(partition)
        if str_part not in self.last_kafka_consumed[topic] or offset > self.last_kafka_consumed[topic][str_part]:
            self.last_kafka_consumed[topic][str_part] = offset

    def get_snapshot_data(self, operator):
        snapshot_data = {}
        if operator in self.last_kafka_consumed.keys():
            snapshot_data['last_kafka_consumed'] = self.last_kafka_consumed[operator]
        return snapshot_data
