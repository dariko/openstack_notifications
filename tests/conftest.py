from openstack_events import EventManager
import openstack
import re
import pytest
import logging
import docker
import time
import kombu

log = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption("--rabbitmq_url", default=None)
    parser.addoption("--os_cloud", default=None)


class RabbitMQContainer:
    def __init__(self, ident=''):
        self.container = docker.from_env().containers.create(
            image='rabbitmq:3.7.10-alpine',
            name='rabbitmq_test_%s' % ident)
        self.container.start()
        self.wait()

        self.add_user('username', 'password')
        self.kombu = kombu.Connection(
            'amqp://username:password@%s' % self.get_ip(),
            failover_strategy='round-robin',
            hearthbeat=1)

    def get_ip(self):
        self.container.reload()
        networks = self.container.attrs['NetworkSettings']['Networks'].values()
        network = list(networks)[0]
        return network['IPAddress']
        return networks

    def add_user(self, username, password):
        log.debug(self.container.exec_run(
            "rabbitmqctl add_user username password"))
        log.debug(self.container.exec_run(
            "rabbitmqctl set_user_tags username administrator"))
        log.debug(self.container.exec_run(
            "rabbitmqctl set_permissions -p / username '.*' '.*' '.*'"))

    def wait(self):
        while b"Server startup complete" not in self.container.logs():
            time.sleep(0.5)

    def url(self):
        return "amqp://username:password@%s" % self.get_ip()

    def publish(self, data, exchange, queue, routing_key, add_timestamp=True):
        if add_timestamp:
            data['timestamp'] = time.strftime(
                '%Y-%m-%d %H:%M:%S.000', time.gmtime())

        _exchange = kombu.Exchange(exchange, 'topic', durable=False)
        _queue = kombu.Queue(queue, exchange=_exchange,
                             routing_key=routing_key, durable=False)
        with kombu.Connection('amqp://username:password@%s' %
                              self.get_ip()) as conn:
            producer = conn.Producer(serializer='json')
            producer.publish(data, exchange=_exchange,
                             routing_key=routing_key,
                             declare=[_queue])

    def port_create(self, port_id):
        self.publish(
            {'event_type': 'port.create.end',
             'payload': {'port': {'id': port_id}}},
            'neutron', 'notifications.neutron', 'notifications.neutron')

    def port_update(self, port_id):
        self.publish(
            {'event_type': 'port.update.end',
             'payload': {'port': {'id': port_id}}},
            'neutron', 'notifications.neutron', 'notifications.neutron')

    def port_delete(self, port_id):
        self.publish(
            {'event_type': 'port.delete.end',
             'payload': {'port': {'id': port_id}}},
            'neutron', 'notifications.neutron', 'notifications.neutron')

    def network_create(self, network_id):
        self.publish(
            {'event_type': 'network.update.end',
             'payload': {'network': {'id': network_id}}},
            'neutron', 'notifications.neutron', 'notifications.neutron')

    def network_update(self, network_id):
        self.publish(
            {'event_type': 'network.update.end',
             'payload': {'network': {'id': network_id}}},
            'neutron', 'notifications.neutron', 'notifications.neutron')

    def network_delete(self, network_id):
        self.publish(
            {'event_type': 'network.delete.end',
             'payload': {'network': {'id': network_id}}},
            'neutron', 'notifications.neutron', 'notifications.neutron')


@pytest.fixture
def rabbitmq_url(request):
    return request.config.getoption("--rabbitmq_url")


@pytest.fixture
def os_cloud(request):
    return request.config.getoption("--os_cloud")


@pytest.fixture
def openstack_client(request, os_cloud):
    if os_cloud is not None:
        return openstack.connect(cloud=os_cloud)


@pytest.fixture
def rabbitmq_container(request):
    function_name = request.function.__name__
    function_name = re.sub(r"[^a-zA-Z0-9]+", "", function_name)
    r = RabbitMQContainer(function_name)

    def fin():
        r.container.remove(force=True)

    request.addfinalizer(fin)
    r.container.reload()
    return r


@pytest.fixture
def event_manager_builder(request):
    managers = []

    def _event_manager_builder(**kwargs):
        em = EventManager(
            **kwargs)
        managers.append(em)
        return em

    def fin():
        for m in managers:
            m.stop()

    request.addfinalizer(fin)
    return _event_manager_builder
