from argparse import ArgumentParser
from .event_manager import EventManager
from .event_manager import CallbackData
import time
import logging

log = logging.getLogger(__name__)


def openstack_events_monitor() -> None:
    parser = ArgumentParser()
    parser.description = 'openstack notifications monitor'
    parser.add_argument('--neutron_exchange', default='neutron')
    parser.add_argument('--neutron_queue', default='notifications.info')
    parser.add_argument('--neutron_routing_key', default='*')
    parser.add_argument('--nova_exchange', default='nova')
    parser.add_argument('--nova_queue', default='notifications.info')
    parser.add_argument('--nova_routing_key', default='*')
    parser.add_argument('--min_timestamp', default=time.mktime(time.gmtime()))
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--rabbitmq_url', required=True)

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    def callback(data: CallbackData) -> None:
        log.info('%s' % data)

    m = EventManager(url=args.rabbitmq_url,
                     neutron_exchange=args.neutron_exchange,
                     neutron_queue=args.neutron_queue,
                     neutron_routing_key=args.neutron_routing_key,
                     nova_exchange=args.nova_exchange,
                     nova_queue=args.nova_queue,
                     nova_routing_key=args.nova_routing_key,
                     min_timestamp=args.min_timestamp,
                     callback=callback)

    log.info('start monitoring')
    m.start()
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    log.info('stop monitoring')
    m.stop()
