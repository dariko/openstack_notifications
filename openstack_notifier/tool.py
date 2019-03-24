from argparse import ArgumentParser, Namespace
from .notifier import OpenstackNotifier, QueueConfig
from .notifier import CallbackData
from typing import List
import time
import logging

log = logging.getLogger(__name__)


def parse_args(args):  # type: (List[str]) -> Namespace
    parser = ArgumentParser()
    parser.description = 'openstack notifications monitor'
    parser.add_argument('--queue_config', action='append', default=[],
                        help='ex: "--queue_config neutron:notifications.info:*'
                        '--queue_config nova:notifications.info:*"')

    parser.add_argument('--min_timestamp', default=time.mktime(time.gmtime()))
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--rabbitmq_url', required=True)
    parsed_args = parser.parse_args(args)

    queue_configs = []
    for s in parsed_args.queue_config:
        parts = s.split(':')
        queue_configs.append(QueueConfig(parts[0], parts[1], parts[2]))
    parsed_args.queue_config = queue_configs

    return parsed_args


def openstack_notifier_tool(sys_args=[]):  # type: (List[str]) -> None
    args = parse_args(sys_args)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    def callback(data):  # type: (CallbackData) -> None
        log.info('%s' % data)

    notifier = OpenstackNotifier(
        url=args.rabbitmq_url,
        queue_configs=args.queue_config,
        min_timestamp=args.min_timestamp,
        callback=callback)

    log.info('start monitoring')
    notifier.start()
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    log.info('stop monitoring')
    notifier.stop()
