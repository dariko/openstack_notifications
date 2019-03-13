from time import sleep
import pytest
import logging

log = logging.getLogger(__name__)


@pytest.mark.skipif(
    pytest.config.getoption("--os_cloud") is None
    or pytest.config.getoption("--rabbitmq_url") is None,
    reason="need --os-cloud and --rabbitmq_url options to run")
@pytest.mark.timeout(30)
def test_live_events(event_manager_builder, mocker,
                     openstack_client, rabbitmq_url):
    on_port_set = mocker.stub(name='on_port_set')
    on_port_del = mocker.stub(name='on_port_del')
    on_network_set = mocker.stub(name='on_network_set')
    on_network_del = mocker.stub(name='on_network_del')
    om = event_manager_builder(url=rabbitmq_url,
                               on_port_set=on_port_set,
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
    network = openstack_client.create_network(name='test_openstack_events')
    log.info('create port')
    port = openstack_client.create_port(network_id=network.id)
    log.info('delete port')
    openstack_client.delete_port(port)
    log.info('delete network')
    openstack_client.delete_network(network)
    sleep(5)
    assert {network.id} in [x[0][0] for x in on_network_set.call_args_list]
    assert {network.id} in [x[0][0] for x in on_network_del.call_args_list]
    assert {port.id} in [x[0][0] for x in on_port_set.call_args_list]
    assert {port.id} in [x[0][0] for x in on_port_del.call_args_list]


@pytest.mark.timeout(120)
def test_generated_events(event_manager_builder, rabbitmq_container, mocker):
    on_port_set = mocker.stub(name='on_port_set')
    on_port_del = mocker.stub(name='on_port_del')
    on_network_set = mocker.stub(name='on_network_set')
    on_network_del = mocker.stub(name='on_network_del')
    om = event_manager_builder(url=rabbitmq_container.url(),
                               on_port_set=on_port_set,
                               on_port_del=on_port_del,
                               on_network_set=on_network_set,
                               on_network_del=on_network_del,
                               )
    om.start()
    sleep(1)

    log.info('create network')
    rabbitmq_container.network_create('0000000000')
    sleep(1)
    on_network_set.assert_called_with({'0000000000'})
    on_network_set.reset_mock()
    log.info('update network')
    rabbitmq_container.network_update('0000000000')
    sleep(1)
    on_network_set.assert_called_with({'0000000000'})
    on_network_set.reset_mock()
    log.info('delete network')
    rabbitmq_container.network_delete('0000000000')
    sleep(1)
    on_network_del.assert_called_with({'0000000000'})
    on_network_del.reset_mock()

    log.info('create port')
    rabbitmq_container.port_create('0000000000')
    sleep(1)
    on_port_set.assert_called_with({'0000000000'})
    on_port_set.reset_mock()
    log.info('update port')
    rabbitmq_container.port_update('0000000000')
    sleep(1)
    on_port_set.assert_called_with({'0000000000'})
    on_port_set.reset_mock()
    log.info('delete port')
    rabbitmq_container.port_delete('0000000000')
    sleep(1)
    on_port_del.assert_called_with({'0000000000'})
    on_port_del.reset_mock()
