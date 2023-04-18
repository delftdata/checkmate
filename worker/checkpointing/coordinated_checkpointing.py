from universalis.common.logging import logging

class CoordinatedCheckpointing:
    def __init__(self):
        self.id = -1
        self.peers = {}

        self.last_kafka_consumed = {}
        #Used to start the coordinated checkpointing
        self.source_operators = set()
        #Used to forward the checkpointing algorithm (operator > (id, operator))
        self.outgoing_channels = {}
        #Used to check whether all markers are received (operator > (id, operator) > boolean)
        self.incoming_channels = {}
        #Used to check whether the coordinator round is done
        self.sink_operators = {}

    async def set_id(self, id):
        self.id = id

    async def set_peers(self, peers):
        self.peers = peers

    async def get_worker_id(self, host, port):
        worker_id = self.id
        for id in self.peers.keys():
            if (host, port) == self.peers[id]:
                worker_id = id
                break
        return worker_id
    
    async def get_source_operators(self):
        return self.source_operators
    
    async def get_outgoing_channels(self, operator):
        if operator in self.outgoing_channels.keys():
            return self.outgoing_channels[operator]
        return set()
    
    async def set_outgoing_channels(self, own_operator, host, port, outgoing_operator):
        if own_operator in self.sink_operators.keys():
            self.sink_operators.pop(own_operator)
        id = await self.get_worker_id(host, port)
        if own_operator not in self.outgoing_channels.keys():
            self.outgoing_channels[own_operator] = set()
        self.outgoing_channels[own_operator].add((id, outgoing_operator))
        return self.outgoing_channels

    async def set_incoming_channels(self, own_operator, sender_id, sender_operator):
        if own_operator not in self.outgoing_channels.keys():
            self.sink_operators[own_operator] = False
        if own_operator not in self.incoming_channels.keys():
            self.incoming_channels[own_operator] = {}
        if (sender_id, sender_operator) not in self.incoming_channels[own_operator].keys():
            self.incoming_channels[own_operator][(sender_id, sender_operator)] = False

    async def marker_received(self, message):
        sender_id, sender_operator, own_operator = message
        if own_operator in self.incoming_channels.keys() and (sender_id, sender_operator) in self.incoming_channels[own_operator].keys():
            self.incoming_channels[own_operator][(sender_id, sender_operator)] = True
            all_markers_received = True
            for channel in self.incoming_channels[own_operator].keys():
                all_markers_received = all_markers_received and self.incoming_channels[own_operator][channel]
            if all_markers_received:
                for channel in self.incoming_channels[own_operator].keys():
                    self.incoming_channels[own_operator][channel] = False
                return True
            else:
                return False
        else:
            logging.warning('received marker from non-incoming channel.')
            return False

    async def set_sink_operator(self, operator):
        logging.warning(f'sink operators: {self.sink_operators}')
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

    def set_consumed_offset(self, topic, partition, offset):
        if topic not in self.source_operators:
            self.source_operators.add(topic)
        if topic not in self.last_kafka_consumed:
            self.last_kafka_consumed[topic] = {}
        self.last_kafka_consumed[topic][str(partition)] = offset

    async def get_snapshot_data(self, operator):
        snapshot_data = {}
        if operator in self.last_kafka_consumed.keys():
            snapshot_data['last_kafka_consumed'] = self.last_kafka_consumed[operator]
        return snapshot_data