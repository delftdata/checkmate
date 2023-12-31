import pytest
import os

# There is some weird import issue with pytest for which I need to change the coordinator import to coordinator.scheduler.round_robin

os.environ['MINIO_HOST'] = '127.0.0.1'
os.environ['MINIO_PORT'] = '9000'
os.environ['MINIO_ROOT_USER'] = 'minio'
os.environ['MINIO_ROOT_PASSWORD'] = 'minio123'

from coordinator.coordinator_service import CoordinatorService

dummy_coordinator = CoordinatorService()

# Create the components for a basic recovery

basic_recovery_graph = {
    ('1', 'filter', 0): set([('1', 'filter', 1)]),
    ('1', 'filter', 2): set(),
    ('1', 'map', 0): set([('1', 'map', 1)]),
    ('1', 'filter', 1): set([('1', 'filter', 2)]),
    ('1', 'map', 1): set(),
    ('2', 'filter', 0): set([('1', 'filter', 1)]),
    ('2', 'map', 0): set(),
}

basic_recovery_graph_root_set = {
    '1': {
        'filter': 2,
        'map': 1
    },
    '2': {
        'filter': 0,
        'map': 0
    }
}

basic_recovery_graph_snapshot_timestamps = {
    '1': {
        'filter': [0, 1, 2],
        'map': [0, 1]
    },
    '2': {
        'filter': [0],
        'map': [0]
    }
}

# Test find_reachable_nodes
@pytest.mark.asyncio
async def test_find_reachable_nodes_empty():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    node = ('2', 'map', 0)
    result = await dummy_coordinator.find_reachable_nodes(node, 7)
    expected_result = set()
    assert result == expected_result

@pytest.mark.asyncio
async def test_find_reachable_nodes_non_empty():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    node = ('1', 'map', 0)
    result = await dummy_coordinator.find_reachable_nodes(node, 7)
    expected_result = set([('1', 'map', 1)])
    assert result == expected_result

@pytest.mark.asyncio
async def test_find_reachable_nodes_recursion():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    node = ('1', 'filter', 0)
    result = await dummy_coordinator.find_reachable_nodes(node, 7)
    expected_result = set([('1', 'filter', 1), ('1', 'filter', 2)])
    assert result == expected_result

# test find_recovery_line (not that this will fail if find_reachable_nodes fails)
@pytest.mark.asyncio
async def test_find_recovery_line():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    dummy_coordinator.recovery_graph_root_set = basic_recovery_graph_root_set
    dummy_coordinator.snapshot_timestamps = basic_recovery_graph_snapshot_timestamps
    expected_recovery_line = {
        '1': {
            'filter': 0,
            'map': 1
        },
        '2': {
            'filter': 0,
            'map': 0
        }
    }
    await dummy_coordinator.find_recovery_line()
    assert dummy_coordinator.recovery_graph_root_set == expected_recovery_line

# Create a scenario without any orphan messages

no_orphan_recovery_graph = {
    ('1', 'filter', 0): set([('1', 'filter', 1)]),
    ('1', 'filter', 1): set([('1', 'filter', 2)]),
    ('1', 'filter', 2): set(),
    ('1', 'map', 0): set([('1', 'map', 1)]),
    ('1', 'map', 1): set([('1', 'map', 2)]),
    ('1', 'map', 2): set(),
    ('2', 'filter', 0): set([('2', 'filter', 1)]),
    ('2', 'filter', 1): set(),
    ('2', 'map', 0): set([('2', 'map', 1)]),
    ('2', 'map', 1): set()
}

no_orphan_recovery_graph_root_set = {
    '1': {
        'filter': 2,
        'map': 2
    },
    '2': {
        'filter': 1,
        'map': 1
    }
}

no_orphan_recovery_graph_snapshot_timestamps = {
    '1': {
        'filter': [0, 1, 2],
        'map': [0, 1, 2]
    },
    '2': {
        'filter': [0, 1],
        'map': [0, 1]
    }
}

@pytest.mark.asyncio
async def test_find_recovery_line_no_orphan():
    dummy_coordinator.recovery_graph = no_orphan_recovery_graph
    dummy_coordinator.recovery_graph_root_set = no_orphan_recovery_graph_root_set
    dummy_coordinator.snapshot_timestamps = no_orphan_recovery_graph_snapshot_timestamps
    expected_root_set = {
        '1': {
            'filter': 2,
            'map': 2
        },
        '2': {
            'filter': 1,
            'map': 1
        }
    }
    await dummy_coordinator.find_recovery_line()
    assert dummy_coordinator.recovery_graph_root_set == expected_root_set

# Create a domino effect scenario

domino_recovery_graph = {
    ('1', 'filter', 0): set([('2', 'filter', 1), ('1', 'filter', 1)]),
    ('1', 'filter', 1): set([('1', 'filter', 2)]),
    ('1', 'filter', 2): set([('1', 'map', 1)]),
    ('1', 'map', 0): set([('1', 'map', 1)]),
    ('1', 'map', 1): set([('1', 'map', 2), ('2', 'filter', 1)]),
    ('1', 'map', 2): set(),
    ('2', 'filter', 0): set([('2', 'filter', 1)]),
    ('2', 'filter', 1): set([('2', 'map', 1), ('1', 'filter', 1), ('2', 'filter', 2)]),
    ('2', 'filter', 2): set(),
    ('2', 'map', 0): set([('1', 'map', 1), ('2', 'map', 1)]),
    ('2', 'map', 1): set([('2', 'map', 2)]),
    ('2', 'map', 2): set()
}

domino_recovery_graph_root_set = {
    '1': {
        'filter': 2,
        'map': 2
    },
    '2': {
        'filter': 2,
        'map': 2
    }
}

domino_recovery_graph_snapshot_timestamps = {
    '1': {
        'filter': [0, 1, 2],
        'map': [0, 1, 2]
    },
    '2': {
        'filter': [0, 1, 2],
        'map': [0, 1, 2]
    }
}

@pytest.mark.asyncio
async def test_find_recovery_line_domino():
    dummy_coordinator.recovery_graph = domino_recovery_graph
    dummy_coordinator.recovery_graph_root_set = domino_recovery_graph_root_set
    dummy_coordinator.snapshot_timestamps = domino_recovery_graph_snapshot_timestamps
    expected_root_set = {
        '1': {
            'filter': 0,
            'map': 0
        },
        '2': {
            'filter': 0,
            'map': 0
        }
    }
    await dummy_coordinator.find_recovery_line()
    assert dummy_coordinator.recovery_graph_root_set == expected_root_set

# test add_edges_between_workers

# also reset some previously set values
dummy_coordinator.recovery_graph_root_set = {}
dummy_coordinator.snapshot_timestamps = {}

recovery_graph_no_messages = {
    ('1', 'filter', 0): set([('1', 'filter', 1)]),
    ('1', 'filter', 1): set([('1', 'filter', 2)]),
    ('1', 'filter', 2): set(),
    ('1', 'map', 0): set([('1', 'map', 1)]),
    ('1', 'map', 1): set([('1', 'map', 2)]),
    ('1', 'map', 2): set(),
    ('2', 'filter', 0): set([('2', 'filter', 1)]),
    ('2', 'filter', 1): set(),
    ('2', 'map', 0): set([('2', 'map', 1)]),
    ('2', 'map', 1): set([('2', 'map', 2)]),
    ('2', 'map', 2): set()
}

# Messages sent/received intervals will differ per test, so they are defined on test level

@pytest.mark.asyncio
async def test_simple_edges():
    dummy_coordinator.recovery_graph = recovery_graph_no_messages
    # Msg received/sent contans the following mapping; workerid > operator > channel > ordered list of (offset, timestamp)
    simple_msg_recieved = {
        '1': {
            'filter': {
                'channel1': [(0, 0), (20, 1)]
            }
        },
        '2': {

        }
    }
    simple_msg_sent = {
        '1': {

        },
        '2': {
            'map': {
                'channel1': [(0, 0), (18, 1), (25, 2)]
            }
        }
    }
    # This simple case should add an edge from (2,0) to (1,1)
    dummy_coordinator.messages_received_intervals = simple_msg_recieved
    dummy_coordinator.messages_sent_intervals = simple_msg_sent
    await dummy_coordinator.add_edges_between_workers()

    expected_result = {
        ('1', 'filter', 0): set([('1', 'filter', 1)]),
        ('1', 'filter', 1): set([('1', 'filter', 2)]),
        ('1', 'filter', 2): set(),
        ('1', 'map', 0): set([('1', 'map', 1)]),
        ('1', 'map', 1): set([('1', 'map', 2)]),
        ('1', 'map', 2): set(),
        ('2', 'filter', 0): set([('2', 'filter', 1)]),
        ('2', 'filter', 1): set(),
        ('2', 'map', 0): set([('1', 'filter', 1), ('2', 'map', 1)]),
        ('2', 'map', 1): set([('1', 'filter', 1), ('2', 'map', 2)]),
        ('2', 'map', 2): set()
    }

    assert expected_result == dummy_coordinator.recovery_graph

same_interval_recovery_graph = {
    ('1', 'filter', 0): set([('1', 'filter', 1)]),
    ('1', 'filter', 1): set([('1', 'filter', 2)]),
    ('1', 'filter', 2): set(),
    ('1', 'map', 0): set([('1', 'map', 1)]),
    ('1', 'map', 1): set([('1', 'map', 2)]),
    ('1', 'map', 2): set(),
    ('2', 'filter', 0): set([('2', 'filter', 1)]),
    ('2', 'filter', 1): set(),
    ('2', 'map', 0): set([('2', 'map', 1)]),
    ('2', 'map', 1): set([('2', 'map', 2)]),
    ('2', 'map', 2): set()
}

@pytest.mark.asyncio
async def test_same_interval_ends():
    dummy_coordinator.recovery_graph = same_interval_recovery_graph

    # If the interval borders are exactly the same, only edges between those intervals should be added.
    same_interval_rec = {
        '1': {
            'filter': {
                'channel1': [(0, 0), (20, 1), (25, 2)]
            }
        },
        '2': {

        }
    }
    same_interval_sent = {
        '1': {

        },
        '2': {
            'map': {
                'channel1': [(0, 0), (20, 1), (25, 2)]
            }
        }
    }

    dummy_coordinator.messages_received_intervals = same_interval_rec
    dummy_coordinator.messages_sent_intervals = same_interval_sent
    await dummy_coordinator.add_edges_between_workers()

    same_interval_res = {
        ('1', 'filter', 0): set([('1', 'filter', 1)]),
        ('1', 'filter', 1): set([('1', 'filter', 2)]),
        ('1', 'filter', 2): set(),
        ('1', 'map', 0): set([('1', 'map', 1)]),
        ('1', 'map', 1): set([('1', 'map', 2)]),
        ('1', 'map', 2): set(),
        ('2', 'filter', 0): set([('2', 'filter', 1)]),
        ('2', 'filter', 1): set(),
        ('2', 'map', 0): set([('1', 'filter', 1), ('2', 'map', 1)]),
        ('2', 'map', 1): set([('1', 'filter', 2), ('2', 'map', 2)]),
        ('2', 'map', 2): set()
    }

    assert same_interval_res == dummy_coordinator.recovery_graph

@pytest.mark.asyncio
async def test_clear_checkpoint_details():
    # Set some dummy values in the coordinator first
    # Then call clear_checkpoint_details to see if it resets correctly
    # Mock root_set, messages_to_replay, message_received_intervals and message_sent_intervals

    clear_cp_recovery_graph = {
        ('1', 'filter', 0): set([('1', 'filter', 1)]),
        ('1', 'filter', 1): set(),
        ('1', 'map', 0): set([('1', 'map', 1)]),
        ('1', 'map', 1): set(),
        ('2', 'filter', 0): set([('2', 'filter', 1)]),
        ('2', 'filter', 1): set(),
        ('2', 'map', 0): set([('2', 'map', 1)]),
        ('2', 'map', 1): set()
    }

    clear_cp_root_set = {
        '1': {
            'filter': 1,
            'map': 1
        },
        '2': {
            'filter': 1,
            'map': 1
        }
    }

    clear_cp_msg_to_replay = {
        '1': {
            'map': {
                0: [],
                1: []
            },
            'filter': {
                0: [],
                1: [0, 1]
            }
        },
        '2': {
            'map': {
                0: [],
                1: []
            },
            'filter': {
                0: [],
                1: [0, 1]
            }
        }
    }

    clear_cp_msg_rec_interval = {
        '1': {
            'filter': {},
            'map': {
                'channel1': [(20, 0), (37, 1)]
            }
        },
        '2': {
            'filter': {},
            'map': {
                'channel2': [(12, 0), (25, 1)]
            }
        }
    }

    clear_cp_msg_snt_interval = {
        '1': {
            'map': {},
            'filter': {
                'channel2': [(17, 0), (25, 1)]
            }
        },
        '2': {
            'map': {},
            'filter': {
                'channel1': [(28, 0), (37, 1)]
            }
        }
    }

    dummy_coordinator.recovery_graph = clear_cp_recovery_graph
    dummy_coordinator.recovery_graph_root_set = clear_cp_root_set
    dummy_coordinator.messages_to_replay = clear_cp_msg_to_replay
    dummy_coordinator.messages_received_intervals = clear_cp_msg_rec_interval
    dummy_coordinator.messages_sent_intervals = clear_cp_msg_snt_interval

    await dummy_coordinator.clear_checkpoint_details()

    graph_after_clear = {
        ('1', 'filter', 1): set(),
        ('1', 'map', 1): set(),
        ('2', 'filter', 1): set(),
        ('2', 'map', 1): set()
    }

    msg_rec_after_clear = {
        '1': {
            'filter': {},
            'map': {
                'channel1': [(37, 1)]
            }
        },
        '2': {
            'filter': {},
            'map': {
                'channel2': [(25, 1)]
            }
        }
    }

    msg_snt_after_clear = {
        '1': {
            'map': {},
            'filter': {
                'channel2': [(25, 1)]
            }
        },
        '2': {
            'map': {},
            'filter': {
                'channel1': [(37, 1)]
            }
        }
    }

    msg_to_replay_after_clear = {
        '1': {
            'map': {
                1: []
            },
            'filter': {
                1: [0, 1]
            }
        },
        '2': {
            'map': {
                1: []
            },
            'filter': {
                1: [0, 1]
            }
        }
    }

    assert msg_to_replay_after_clear == dummy_coordinator.messages_to_replay
    assert msg_snt_after_clear == dummy_coordinator.messages_sent_intervals
    assert msg_rec_after_clear == dummy_coordinator.messages_received_intervals
    assert graph_after_clear == dummy_coordinator.recovery_graph

@pytest.mark.asyncio
async def test_find_channels_to_replay():
    # Mock recovery graph root set, messages_to_replay and partitions_to_ids
    simple_root_set = {
        '1': {
            'filter': 3,
            'map': 2
        },
        '2': {
            'filter': 3,
            'map': 1
        }
    }

    partitions_to_ids = {
        'filter': {
            '0': 1,
            '1': 2
        },
        'map': {
            '0': 1,
            '1': 2
        }
    }

    # This basically states; for worker_id 1 and snapshot_timestamp 3, channel operator1_operator2_2 has to be replayed from offset 12
    # Note that the channel number is determined by the partitions_to_ids (since channel number = send_op_part*len(rec_op)+rec_op_part)
    # An example for msg_to_replay['1'][3] for channel operator1_operator2_x, given that we want it to be sent by worker_id 2, using mock partitions_to_ids:
    # Operator2 is the receiving operator in this case and hosted by worker_id 1 and operator1 is sending op hosted by worker_id 2,
    # thus our formula looks like; 1 * 2 + 0 = 2, thus the channel_no in msg_to_replay should be 2.
    msg_to_replay = {
        '1': {
            'filter': {
                3: {
                    'filter_map_2': 12
                }
            },
            'map': {
                2: {}
            }
        },
        '2': {
            'filter': {
                3: {
                    'filter_map_1': 17
                }
            },
            'map': {
                1: {}
            }
        }
    }
   
    dummy_coordinator.recovery_graph_root_set = simple_root_set
    dummy_coordinator.partitions_to_ids = partitions_to_ids
    dummy_coordinator.messages_to_replay = msg_to_replay

    result = await dummy_coordinator.find_channels_to_replay()

    # We created the mock such that the channels to replayed for both snapshots are sent by the other worker_id
    # Thus the channels in msg_to_replay should simply be swapped in this case.
    expected_result = {
        '1': {
            'filter': (3, {'filter_map_1': 17}),
            'map': (2, {})
        },
        '2': {
            'filter': (3, {'filter_map_2': 12}),
            'map': (1, {})
        }
    }

    assert result == expected_result