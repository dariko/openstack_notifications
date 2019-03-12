from time import sleep
import pytest
import logging

log = logging.getLogger(__name__)


@pytest.mark.timeout(30)
def test_event_manager_flow(event_manager_builder, mocker):
    on_port_set = mocker.stub(name='on_port_set')
    on_port_del = mocker.stub(name='on_port_del')
    on_network_set = mocker.stub(name='on_network_set')
    on_network_del = mocker.stub(name='on_network_del')
    om = event_manager_builder(on_port_set=on_port_set,
                               on_port_del=on_port_del,
                               on_network_set=on_network_set,
                               on_network_del=on_network_del,
                               )
    om.start()
    sleep(5)
    on_port_set.reset_mock()
    on_port_del.reset_mock()
    on_network_set.reset_mock()
    on_network_del.reset_mock()
    log.info('create network')
    openstack = om.openstack
    network = openstack.create_network(name='test_openstack_manager')
    log.info('create port')
    port = openstack.create_port(network_id=network.id)
    log.info('delete port')
    openstack.delete_port(port)
    log.info('delete network')
    openstack.delete_network(network)
    sleep(5)
    assert {network.id} in [x[0][0] for x in on_network_set.call_args_list]
    assert {network.id} in [x[0][0] for x in on_network_del.call_args_list]
    assert {port.id} in [x[0][0] for x in on_port_set.call_args_list]
    assert {port.id} in [x[0][0] for x in on_port_del.call_args_list]
