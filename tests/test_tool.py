# from multiprocessing import Process
# from openstack_notifier.tool import openstack_notifier_tool
from openstack_notifier.tool import parse_args

import pytest
import logging

log = logging.getLogger(__name__)


def test_tool_missing_rabbitmq_url():
    with pytest.raises(SystemExit):
        parse_args([])


def test_tool_debug():
    args = parse_args(['--rabbitmq_url', 'foo', '--debug'])
    assert getattr(args, 'debug')
    args = parse_args(['--rabbitmq_url', 'foo'])
    assert not getattr(args, 'debug')


@pytest.mark.parametrize("arg_name, arg_value",
                         [('neutron_exchange', 'foo'),
                          ('neutron_queue', 'foo'),
                          ('neutron_routing_key', 'foo'),
                          ('nova_exchange', 'foo'),
                          ('nova_queue', 'foo'),
                          ('nova_routing_key', 'foo'),
                          ('min_timestamp', '0'),
                          ])
def test_tool_args(arg_name, arg_value):
    args = parse_args([
        '--rabbitmq_url', 'foo', '--%s' % arg_name, arg_value])
    assert getattr(args, arg_name) == arg_value


# should start a rabbitmq container and test connection to it
# def test_tool_start():
#     p = Process(target=openstack_notifier_tool,
#                 args=(['--rabbitmq_url', 'amqp://127.0.0.1:3333'],))
#     p.start()
#     sleep(1)
#     assert p.is_alive()
#     p.terminate()
#     sleep(1)
#     assert not p.is_alive()
