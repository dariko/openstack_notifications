from .stoppable_thread import StoppableThread
from typing import Optional, Dict, Any, Callable, Set
import time
import logging
import kombu  # type: ignore
import socket

log = logging.getLogger(__name__)


OpenstackManagerCallback = Optional[Callable[[Set[str]], None]]


class EventManager(StoppableThread):
    def __init__(self,
                 url: str,
                 on_port_set: OpenstackManagerCallback = None,
                 on_port_del: OpenstackManagerCallback = None,
                 on_network_set: OpenstackManagerCallback = None,
                 on_network_del: OpenstackManagerCallback = None,
                 neutron_exchange: str = "neutron",
                 neutron_topic: str = "notifications.info",
                 min_timestamp: Optional[float] = None,
                 ):
        self.url: str = url
        self.on_port_set = on_port_set
        self.on_port_del = on_port_del
        self.on_network_set = on_network_set
        self.on_network_del = on_network_del
        if min_timestamp is None:
            self.min_timestamp = time.mktime(time.gmtime())
        else:
            self.min_timestamp = min_timestamp
        super().__init__()

        self.rabbitmq = kombu.Connection(
            self.url, failover_strategy='round-robin',
            connect_timeout=2,
            hearthbeat=1)
        self.rabbitmq.ensure_connection(max_retries=3)

    def rabbitmq_callback(self, body: Dict[str, Any], message: str) -> None:
        try:
            event_type = body.get('event_type', None)
            event_ts_s = body.get('timestamp', None)
            if event_type is None:
                return
            if event_ts_s is None:
                log.debug('message has no timestamp, skipping: %s'
                          % body)
                return
            event_ts = time.mktime(time.strptime(
                event_ts_s, '%Y-%m-%d %H:%M:%S.%f'))
            if self.min_timestamp > event_ts:
                log.debug('old message, skipping: %s'
                          % body)
                return
            if event_type == 'port.delete.end':
                port_id = body['payload']['port']['id']
                if self.on_port_del is not None:
                    self.on_port_del({port_id})
            elif event_type == 'port.create.end' or \
                    event_type == 'port.update.end':
                port_id = body['payload']['port']['id']
                if self.on_port_set is not None:
                    self.on_port_set({port_id})
            elif event_type == 'network.delete.end':
                network_id = body['payload']['network']['id']
                if self.on_network_del is not None:
                    self.on_network_del({network_id})
            elif event_type == 'network.create.end' or \
                    event_type == 'network.update.end':
                network_id = body['payload']['network']['id']
                if self.on_network_set is not None:
                    self.on_network_set({network_id})
            elif event_type is not None:
                log.debug('skipping message event_type: %s, body: %s' %
                          (event_type, body))
            else:
                log.debug('unknown event: %s' % body)
        except Exception:
            log.exception('Error while parsing message %s' % body)

    def run(self) -> None:
        try:
            log.debug('start listening to exchange neutron,'
                      'queue notifications.neutron, %s' %
                      self.url)
            neutron_ex = kombu.Exchange('neutron', type='topic', durable=False)
            neutron_q = kombu.Queue('notifications.neutron',
                                    exchange=neutron_ex,
                                    routing_key='*',
                                    durable=False)
            self.minimum_timestamp = time.mktime(time.gmtime())
            with self.rabbitmq.Consumer(
                    neutron_q, callbacks=[self.rabbitmq_callback]) as consumer:
                while not self.quit_event.is_set():
                    while not self.quit_event.wait(timeout=1):
                        try:
                            while not self.quit_event.is_set():
                                self.rabbitmq.drain_events(timeout=1)
                        except socket.timeout:
                            self.rabbitmq.heartbeat_check()

        except Exception as e:
            log.exception('error in OpenstackManager: %s' % e)
        finally:
            self.rabbitmq.release()

    def stop(self) -> None:
        super().stop()
        self.join()
