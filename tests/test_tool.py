# from multiprocessing import Process
from openstack_notifier.tool import QueueConfig
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


def test_tool_timestamp():
    url_args = ['--rabbitmq_url', 'foo']
    args = parse_args(url_args + ['--min_timestamp', '0'])
    assert args.min_timestamp == '0'


def test_tool_single_queue():
    url_args = ['--rabbitmq_url', 'foo']
    args = parse_args(url_args + ['--queue_config', 'foo:bar:foobar'])
    assert len(args.queue_config) == 1
    assert args.queue_config[0] == QueueConfig('foo', 'bar', 'foobar')


def test_tool_multiple_queue():
    url_args = ['--rabbitmq_url', 'foo']
    args = parse_args(url_args +
                      ['--queue_config', 'foo:bar:foobar'] +
                      ['--queue_config', 'FOO:BAR:*'])
    assert len(args.queue_config) == 2
    assert args.queue_config[0] == QueueConfig('foo', 'bar', 'foobar')
    assert args.queue_config[1] == QueueConfig('FOO', 'BAR', '*')


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
