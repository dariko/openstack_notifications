from openstack_events import EventManager
import pytest
import logging

log = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption(
        "--openstack_cloud_name", default=None,
        help="to test on live cloud, set to CLOUD_NAME")
    parser.addoption(
        "--rabbitmq_username",
        default="username")
    parser.addoption(
        "--rabbitmq_password",
        default="password")
    parser.addoption(
        "--rabbitmq_url")


@pytest.fixture
def openstack_cloud_name(request):
    return request.config.getoption("--openstack_cloud_name")


@pytest.fixture
def rabbitmq_username(request):
    return request.config.getoption("--rabbitmq_username")


@pytest.fixture
def rabbitmq_password(request):
    return request.config.getoption("--rabbitmq_password")


@pytest.fixture
def rabbitmq_url(request):
    return request.config.getoption("--rabbitmq_url")


@pytest.fixture
def event_manager_builder(request, rabbitmq_url, openstack_cloud_name):
    managers = []

    def _event_manager_builder(**kwargs):
        em = EventManager(
            rabbit_url=rabbitmq_url,
            openstack_options={'cloud': openstack_cloud_name},
            **kwargs)
        managers.append(em)
        return em

    def fin():
        for m in managers:
            m.stop()

    request.addfinalizer(fin)
    return _event_manager_builder
