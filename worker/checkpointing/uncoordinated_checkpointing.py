class UncoordinatedCheckpointing:
    def __init__(self):
        self.id = -1
        self.peers = {}

        self.last_snapshot_timestamp = {}
        self.total_partitions_per_operator = {}
        self.last_messages_sent = {}
        self.last_kafka_consumed = {}
        self.kafka_consumer = None
        self.last_messages_processed = {}

    def set_id(self, id):
        self.id = id

    def set_peers(self, peers):
        self.peers = peers