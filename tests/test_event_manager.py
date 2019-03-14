from time import sleep
from openstack_events.event_manager import CallbackData
import pytest
import logging

log = logging.getLogger(__name__)


def event_in_callbackdata(callbackdata, event_type, payload_section, **kwargs):
    matching_type = [x for x in callbackdata if x.event_type == event_type]
    if len(matching_type) == 0:
        raise AssertionError('event_type %s not found in callbacks %s' %
                             (event_type, callbackdata))
    for c in matching_type:
        match = True
        for k, v in kwargs.items():
            if c.payload.get(payload_section, {}).get(k, None) != v:
                match = False
                continue
        if match:
            return True
    return False


@pytest.mark.live
@pytest.mark.timeout(30)
def test_live_events(event_manager_builder, mocker,
                     openstack_client, rabbitmq_url):
    callbackdata = []

    def callback(data):
        callbackdata.append(data)

    om = event_manager_builder(url=rabbitmq_url,
                               callback=callback,
                               )
    om.start()
    sleep(1)
    log.info('create network')
    network = openstack_client.create_network(name='test_openstack_events')
    log.info('create port')
    port = openstack_client.create_port(network_id=network.id)
    log.info('delete port')
    openstack_client.delete_port(port)
    log.info('delete network')
    openstack_client.delete_network(network)

    sleep(3)
    assert event_in_callbackdata(callbackdata, 'network.create.end',
                                 'network', id=network.id)
    assert event_in_callbackdata(callbackdata, 'network.delete.end',
                                 'network', id=network.id)
    assert event_in_callbackdata(callbackdata, 'port.create.end',
                                 'port', id=port.id)
    assert event_in_callbackdata(callbackdata, 'port.delete.end',
                                 'port', id=port.id)


@pytest.mark.timeout(120)
def test_generated_events(event_manager_builder, rabbitmq_container, mocker):
    callback = mocker.stub(name='callback')
    om = event_manager_builder(url=rabbitmq_container.url(),
                               callback=callback,
                               )
    om.start()
    sleep(1)

    log.info('create network')
    rabbitmq_container.network_create('0000000000')
    while not callback.called:
        log.info('waiting for callback')
        sleep(0.5)
    log.info(callback.call_args_list)
    callback.assert_called_with(
        CallbackData('network.create.end', {'network': {'id': '0000000000'}}))
    callback.reset_mock()

    log.info('update network')
    rabbitmq_container.network_update('0000000000')
    while not callback.called:
        log.info('waiting for callback')
        sleep(0.5)
    log.info(callback.call_args_list)
    callback.assert_called_with(
        CallbackData('network.update.end', {'network': {'id': '0000000000'}}))
    callback.reset_mock()

    log.info('delete network')
    rabbitmq_container.network_delete('0000000000')
    while not callback.called:
        log.info('waiting for callback')
        sleep(0.5)
    log.info(callback.call_args_list)
    callback.assert_called_with(
        CallbackData('network.delete.end', {'network': {'id': '0000000000'}}))
    callback.reset_mock()

    log.info('create port')
    rabbitmq_container.port_create('0000000000')
    while not callback.called:
        log.info('waiting for callback')
        sleep(0.5)
    log.info(callback.call_args_list)
    callback.assert_called_with(
        CallbackData('port.create.end', {'port': {'id': '0000000000'}}))
    callback.reset_mock()

    log.info('update port')
    rabbitmq_container.port_update('0000000000')
    while not callback.called:
        log.info('waiting for callback')
        sleep(0.5)
    log.info(callback.call_args_list)
    callback.assert_called_with(
        CallbackData('port.update.end', {'port': {'id': '0000000000'}}))
    callback.reset_mock()

    log.info('delete port')
    rabbitmq_container.port_delete('0000000000')
    while not callback.called:
        log.info('waiting for callback')
        sleep(0.5)
    log.info(callback.call_args_list)
    callback.assert_called_with(
        CallbackData('port.delete.end', {'port': {'id': '0000000000'}}))
    callback.reset_mock()

    om.stop()
