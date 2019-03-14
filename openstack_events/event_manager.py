from .stoppable_thread import StoppableThread
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass
import time
import logging
import kombu  # type: ignore
import socket

log = logging.getLogger(__name__)


@dataclass
class CallbackData:
    event_type: str
    payload: Dict[str, Any]


EventManagerCallback = Optional[Callable[[CallbackData], None]]


class EventManager(StoppableThread):
    def __init__(self,
                 url: str,
                 callback: EventManagerCallback = None,
                 neutron_exchange: str = "neutron",
                 neutron_topic: str = "notifications.info",
                 min_timestamp: Optional[float] = None,
                 ):
        self.url: str = url
        self.callback = callback
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
            log.debug('received message: %s' % body)
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

            if self.callback is not None:
                payload = body.get('payload', {})
                callback_data = CallbackData(event_type=event_type,
                                             payload=payload)
                self.callback(callback_data)
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
                    neutron_q, callbacks=[self.rabbitmq_callback]):
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
