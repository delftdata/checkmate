import pytest
import os

os.environ['MINIO_HOST'] = '127.0.0.1'
os.environ['MINIO_PORT'] = '9000'
os.environ['MINIO_ROOT_USER'] = 'minio'
os.environ['MINIO_ROOT_PASSWORD'] = 'minio123'

from coordinator.coordinator_service import CoordinatorService

dummy_coordinator = CoordinatorService()

basic_recovery_graph = {
    ('1', 0): set(),
    ('2', 0): set(),
    ('1', 1): set()
}

basic_recovery_graph[('1', 0)].add(('1', 1))
basic_recovery_graph[('2', 0)].add(('1', 1))

basic_recovery_graph_root_set = {
    '1': 1,
    '2': 0
}

@pytest.mark.asyncio
async def test_find_reachable_nodes():
    dummy_coordinator.recovery_graph = basic_recovery_graph
    node = ('1', 0)
    result = await dummy_coordinator.find_reachable_nodes(node)
    expected_result = set([('1', 1)])
    assert result == expected_result
    