import pytest
import os

os.environ['MINIO_HOST'] = '127.0.0.1'
os.environ['MINIO_PORT'] = '9000'
os.environ['MINIO_ROOT_USER'] = 'minio'
os.environ['MINIO_ROOT_PASSWORD'] = 'minio123'

from coordinator.coordinator_service import CoordinatorService

dummy_coordinator = CoordinatorService()

# Create the components for a basic recovery

basic_recovery_graph = {
    ('1', 0): set([('1', 1)]),
    ('2', 0): set([('1', 1)]),
    ('1', 1): set([('1', 2)]),
    ('1', 2): set(),
}

basic_recovery_graph_root_set = {
    '1': 2,
    '2': 0
}

basic_recovery_graph_snapshot_timestamps = {
    '1': [0, 1, 2],
    '2': [0]
}

# Test find_reachable_nodes
@pytest.mark.asyncio
async def test_find_reachable_nodes_empty():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    node = ('1', 2)
    result = await dummy_coordinator.find_reachable_nodes(node)
    expected_result = set()
    assert result == expected_result

@pytest.mark.asyncio
async def test_find_reachable_nodes_non_empty():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    node = ('1', 1)
    result = await dummy_coordinator.find_reachable_nodes(node)
    expected_result = set([('1', 2)])
    assert result == expected_result

@pytest.mark.asyncio
async def test_find_reachable_nodes_recursion():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    node = ('2', 0)
    result = await dummy_coordinator.find_reachable_nodes(node)
    expected_result = set([('1', 1), ('1', 2)])
    assert result == expected_result

# test find_recovery_line (not that this will fail if find_reachable_nodes fails)
@pytest.mark.asyncio
async def test_find_recovery_line():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    dummy_coordinator.recovery_graph_root_set = basic_recovery_graph_root_set
    dummy_coordinator.snapshot_timestamps = basic_recovery_graph_snapshot_timestamps
    await dummy_coordinator.find_recovery_line()
    assert dummy_coordinator.recovery_graph_root_set == {'1': 0, '2': 0}

# Create a scenario without any orphan messages

no_orphan_recovery_graph = {
    ('1', 0): set([('1', 1)]),
    ('2', 0): set([('2', 1)]),
    ('1', 1): set([('1', 2)]),
    ('1', 2): set(),
    ('2', 1): set()
}

no_orphan_recovery_graph_root_set = {
    '1': 2,
    '2': 1
}

no_orphan_recovery_graph_snapshot_timestamps = {
    '1': [0, 1, 2],
    '2': [0, 1]
}

@pytest.mark.asyncio
async def test_find_recovery_line_no_orphan():
    dummy_coordinator.recovery_graph = no_orphan_recovery_graph
    dummy_coordinator.recovery_graph_root_set = no_orphan_recovery_graph_root_set
    dummy_coordinator.snapshot_timestamps = no_orphan_recovery_graph_snapshot_timestamps
    await dummy_coordinator.find_recovery_line()
    assert dummy_coordinator.recovery_graph_root_set == {'1': 2, '2': 1}

# Create a domino effect scenario

domino_recovery_graph = {
    ('1', 0): set([('1', 1)]),
    ('2', 0): set([('2', 1), ('1', 1)]),
    ('1', 1): set([('1', 2), ('2', 1)]),
    ('1', 2): set(),
    ('2', 1): set([('1', 2)])
}

domino_recovery_graph_root_set = {
    '1': 2,
    '2': 1
}

domino_recovery_graph_snapshot_timestamps = {
    '1': [0, 1, 2],
    '2': [0, 1]
}

@pytest.mark.asyncio
async def test_find_recovery_line_domino():
    dummy_coordinator.recovery_graph = domino_recovery_graph
    dummy_coordinator.recovery_graph_root_set = domino_recovery_graph_root_set
    dummy_coordinator.snapshot_timestamps = domino_recovery_graph_snapshot_timestamps
    await dummy_coordinator.find_recovery_line()
    assert dummy_coordinator.recovery_graph_root_set == {'1': 0, '2': 0}

# test add_edges_between_workers

# also reset some previously set values
dummy_coordinator.recovery_graph_root_set = {}
dummy_coordinator.snapshot_timestamps = {}

recovery_graph_no_messages = {
    ('1', 0): set([('1', 1)]),
    ('1', 1): set([('1', 2)]),
    ('1', 2): set([('1', 3)]),
    ('1', 3): set(),
    ('2', 0): set([('2', 1)]),
    ('2', 1): set([('2', 2)]),
    ('2', 2): set([('2', 3)]),
    ('2', 3): set()
}

# Messages sent/received intervals will differ per test, so they are defined on test level

@pytest.mark.asyncio
async def test_simple_edges():
    dummy_coordinator.recovery_graph = recovery_graph_no_messages
    # Msg received/sent contans the following mapping; workerid > channel > ordered list of (offset, timestamp)
    simple_msg_recieved = {
        '1': {
            'channel1': [(0, 0), (20, 1)]
        },
        '2': {

        }
    }
    simple_msg_sent = {
        '1': {

        },
        '2': {
            'channel1': [(0, 0), (18, 1), (25, 2)]
        }
    }
    # This simple case should add an edge from (2,0) to (1,1)
    dummy_coordinator.messages_received_intervals = simple_msg_recieved
    dummy_coordinator.messages_sent_intervals = simple_msg_sent
    await dummy_coordinator.add_edges_between_workers()

    expected_result = {
        ('1', 0): set([('1', 1)]),
        ('1', 1): set([('1', 2)]),
        ('1', 2): set([('1', 3)]),
        ('1', 3): set(),
        ('2', 0): set([('1', 1), ('2', 1)]),
        ('2', 1): set([('1', 1), ('2', 2)]),
        ('2', 2): set([('2', 3)]),
        ('2', 3): set()
    }

    assert expected_result == dummy_coordinator.recovery_graph

new_recovery_graph = {
    ('1', 0): set([('1', 1)]),
    ('1', 1): set([('1', 2)]),
    ('1', 2): set([('1', 3)]),
    ('1', 3): set(),
    ('2', 0): set([('2', 1)]),
    ('2', 1): set([('2', 2)]),
    ('2', 2): set([('2', 3)]),
    ('2', 3): set()
}

@pytest.mark.asyncio
async def test_same_interval_ends():
    dummy_coordinator.recovery_graph = new_recovery_graph

    print(dummy_coordinator.recovery_graph)

    # If the interval borders are exactly the same, only edges between those intervals should be added.
    same_interval_rec = {
        '1': {
            'channel1': [(0, 0), (20, 1), (25, 2)]
        },
        '2': {

        }
    }
    same_interval_sent = {
        '1': {

        },
        '2': {
            'channel1': [(0, 0), (20, 1), (25, 2)]
        }
    }

    dummy_coordinator.messages_received_intervals = same_interval_rec
    dummy_coordinator.messages_sent_intervals = same_interval_sent
    await dummy_coordinator.add_edges_between_workers()

    print(dummy_coordinator.recovery_graph)

    same_interval_res = {
        ('1', 0): set([('1', 1)]),
        ('1', 1): set([('1', 2)]),
        ('1', 2): set([('1', 3)]),
        ('1', 3): set(),
        ('2', 0): set([('1', 1), ('2', 1)]),
        ('2', 1): set([('1', 2), ('2', 2)]),
        ('2', 2): set([('2', 3)]),
        ('2', 3): set()
    }

    assert same_interval_res == dummy_coordinator.recovery_graph

