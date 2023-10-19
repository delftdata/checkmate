import time

from aiokafka import TopicPartition


class UncoordinatedCheckpointing(object):
    def __init__(self):
        self.id = -1
        self.peers = {}

        self.last_snapshot_timestamp = {}
        self.last_messages_sent = {}
        self.last_kafka_consumed = {}
        self.kafka_consumer = None
        self.last_messages_processed = {}

    def set_id(self, _id):
        self.id = _id

    def set_peers(self, peers):
        self.peers = peers

    def set_last_messages_processed(self, operator, channel, offset):
        if (channel not in self.last_messages_processed[operator] or
                offset > self.last_messages_processed[operator][channel]):
            self.last_messages_processed[operator][channel] = offset

    def get_offsets(self):
        return self.last_kafka_consumed

    def set_consumed_offset(self, topic, partition, offset):
        if topic not in self.last_kafka_consumed:
            self.last_kafka_consumed[topic] = {}
        str_part = str(partition)
        if str_part not in self.last_kafka_consumed[topic] or offset > self.last_kafka_consumed[topic][str_part]:
            self.last_kafka_consumed[topic][str_part] = offset

    def init_attributes_per_operator(self, operators):
        for op in operators:
            self.last_messages_processed[op] = {}
            self.last_snapshot_timestamp[op] = time.time_ns() // 1000000

    def get_last_snapshot_timestamp(self, operator):
        return self.last_snapshot_timestamp[operator]

    def get_snapshot_data(self, operator, last_messages_sent):
        snapshot_data = {'last_messages_sent': last_messages_sent,
                         'last_messages_processed': self.last_messages_processed[operator]}
        if operator in self.last_kafka_consumed.keys():
            snapshot_data['last_kafka_consumed'] = self.last_kafka_consumed[operator]
        self.last_messages_processed[operator] = {}
        self.last_snapshot_timestamp[operator] = time.time_ns() // 1000000
        return snapshot_data

    def restore_snapshot_data(self,
                              operator_name,
                              last_messages_processed,
                              last_messages_sent,
                              last_kafka_consumed):
        self.last_messages_processed[operator_name] = last_messages_processed
        self.last_messages_sent[operator_name] = last_messages_sent
        to_replay = []
        if operator_name in self.last_kafka_consumed:
            for partition in self.last_kafka_consumed[operator_name]:
                if partition in last_kafka_consumed:
                    to_replay.append((TopicPartition(operator_name,
                                                     int(partition)),
                                      last_kafka_consumed[partition] + 1))
                else:
                    last_kafka_consumed[partition] = 0
                    to_replay.append((TopicPartition(operator_name, int(partition)), 0))
            self.last_kafka_consumed[operator_name] = last_kafka_consumed
        return to_replay

    def find_last_sent_offset(self, operator, channel):
        replay_until = None
        if channel in self.last_messages_sent[operator]:
            replay_until = self.last_messages_sent[operator][channel]
        return replay_until

    def reset_messages_processed(self, operator):
        self.last_messages_processed[operator] = {}
        tp_to_reset = []
        for partition in self.last_kafka_consumed[operator].keys():
            tp_to_reset.append(TopicPartition(operator, int(partition)))
        return tp_to_reset
