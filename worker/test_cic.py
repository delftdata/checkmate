import pytest
from worker.checkpointing.cic_checkpointing import CICCheckpointing

cic_object_one = CICCheckpointing()
cic_object_two = CICCheckpointing()

# Simulate a cycle that causes a checkpoint to be useless to see if a cycle is detected.
# Actually simulating the messages can be quite tedious, however all the necessary data for the piggybacking process
# is gathered with the CICCheckpointing class. Therefore we can simply create two cic_objects, make the corresponding method calls
# and use to return objects as "message" values.

operators = ['map', 'filter']
ids = [1, 2]


@pytest.mark.asyncio
async def test_cycle_detection():

    await cic_object_one.init_cic(operators, ids)
    await cic_object_two.init_cic(operators, ids)

    peers_one = {
        2: ('0.0.0.0', '9999')
    }

    peers_two = {
        1: ('0.0.0.0', '9998')
    }

    await cic_object_one.set_id(1)
    await cic_object_one.set_peers(peers_one)

    await cic_object_two.set_id(2)
    await cic_object_two.set_peers(peers_two)

    await cic_object_one.update_cic_checkpoint('map')
    await cic_object_two.update_cic_checkpoint('map')
    await cic_object_one.update_cic_checkpoint('filter')
    await cic_object_two.update_cic_checkpoint('filter')

    # After initializing we can now simulate the messages
    # A scenario with a useless checkpoint would be;
    # 1 sends a message to 2, 2 checkpoints after and sends a message to 1.
    # Once the second message is received, 1 should detect a cycle with a useless checkpoint, forcing one to be taken.

    cic_details_message_one = await cic_object_one.get_message_details('0.0.0.0', '9999', 'filter', 'map')

    cycle_detection_one = await cic_object_two.cic_cycle_detection('map', cic_details_message_one)
    assert not cycle_detection_one[0]
    await cic_object_two.update_cic_checkpoint('map')

    cic_details_message_two = await cic_object_two.get_message_details('0.0.0.0', '9998', 'map', 'filter')
    cycle_detection_two = await cic_object_one.cic_cycle_detection('filter', cic_details_message_two)
    assert cycle_detection_two[0]