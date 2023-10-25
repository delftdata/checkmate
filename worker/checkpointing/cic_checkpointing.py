from universalis.common.logging import logging
from worker.checkpointing.uncoordinated_checkpointing import UncoordinatedCheckpointing

class CICCheckpointing(UncoordinatedCheckpointing):
    def __init__(self):
        super().__init__()
        # CIC
        self.sent_to = {}
        self.logical_clock = {}
        self.checkpoint_clocks = {}
        self.taken = {}
        self.greater = {}

    async def init_cic(self, operators, ids):
            for op in operators:
                # CIC
                # Normally the algorithm uses four arrays (three bool, one vector clock) and a logical clock per worker.
                # However our checkpoints are operator based, meaning we need this for every operator separately.
                self.sent_to[op] = {}
                self.logical_clock[op] = 0
                self.taken[op] = {}
                self.greater[op] = {}
                self.checkpoint_clocks[op] = {}
                for id in ids:
                    self.sent_to[op][id] = {}
                    self.taken[op][id] = {}
                    self.greater[op][id] = {}
                    self.checkpoint_clocks[op][id] = {}
                    for op2 in operators:
                        self.sent_to[op][id][op2] = False
                        self.taken[op][id][op2] = False
                        self.greater[op][id][op2] = False
                        self.checkpoint_clocks[op][id][op2] = 0

    def update_cic_checkpoint(self, operator):
        for id in self.sent_to[operator].keys():
            for op in self.sent_to[operator][id].keys():
                self.sent_to[operator][id][op] = False
                if not (op == operator and self.id == id):
                    self.taken[operator][id][op] = True
                    self.greater[operator][id][op] = True
        self.logical_clock[operator] = self.logical_clock[operator] + 1
        self.checkpoint_clocks[operator][self.id][operator] = self.checkpoint_clocks[operator][self.id][operator] + 1
        return self.logical_clock[operator]

    def get_worker_id(self, host, port):
        worker_id = self.id
        for id in self.peers.keys():
            if (host, port) == self.peers[id]:
                worker_id = id
                break
        return worker_id

    def get_cic_logical_clock(self, operator):
        return self.logical_clock[operator]

    def get_message_details(self, host, port, sending_name, rec_name):
        details = {}
        receiver_id = self.get_worker_id(host, port)
        self.sent_to[sending_name][receiver_id][rec_name] = True
        details['__LC__'] = self.logical_clock[sending_name]
        details['__GREATER__'] = self.greater[sending_name]
        details['__TAKEN__'] = self.taken[sending_name]
        details['__CHECKPOINT_CLOCKS__'] = self.checkpoint_clocks[sending_name]
        return details

    def cic_cycle_detection(self, operator, cic_details):
        if cic_details == {}:
            return False, None
        cycle_detected = False
        sent_greater_and = False
        for id in self.sent_to[operator].keys():
            for op in self.sent_to[operator][id].keys():
                if self.sent_to[operator][id][op] and cic_details['__GREATER__'][id][op]:
                    sent_greater_and = True
                    break
            if sent_greater_and:
                break
        if (sent_greater_and and (cic_details['__LC__'] > self.logical_clock[operator])) or ((cic_details['__CHECKPOINT_CLOCKS__'][self.id][operator] == self.checkpoint_clocks[operator][self.id][operator]) and cic_details['__TAKEN__'][self.id][operator]):
            self.update_cic_checkpoint(operator)
            cycle_detected = True

        cic_clock = self.logical_clock[operator]

        # Compare local clocks

        if cic_details['__LC__'] > self.logical_clock[operator]:
            self.logical_clock[operator] = cic_details['__LC__']
            self.greater[operator][self.id][operator] = False
            for id in self.greater[operator].keys():
                for op in self.greater[operator][id].keys():
                    if not (id == self.id and op == operator):
                        self.greater[operator][id][op] = cic_details['__GREATER__'][id][op]
        elif cic_details['__LC__'] == self.logical_clock[operator]:
            for id in self.greater[operator].keys():
                for op in self.greater[operator][id].keys():
                    self.greater[operator][id][op] = self.greater[operator][id][op] and cic_details['__GREATER__'][id][op]

        # Compare checkpoint clocks

        for id in cic_details['__CHECKPOINT_CLOCKS__'].keys():
            for op in cic_details['__CHECKPOINT_CLOCKS__'][id].keys():
                if id == self.id and op == operator:
                    continue
                else:
                    if cic_details['__CHECKPOINT_CLOCKS__'][id][op] > self.checkpoint_clocks[operator][id][op]:
                        self.checkpoint_clocks[operator][id][op] = cic_details['__CHECKPOINT_CLOCKS__'][id][op]
                        self.taken[operator][id][op] = cic_details['__TAKEN__'][id][op]
                    elif cic_details['__CHECKPOINT_CLOCKS__'][id][op] == self.checkpoint_clocks[operator][id][op]:
                        self.taken[operator][id][op] = cic_details['__TAKEN__'][id][op] or self.taken[operator][id][op]

        return cycle_detected, cic_clock