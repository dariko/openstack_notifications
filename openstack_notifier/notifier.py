from threading import Thread, Event
from typing import Optional, Dict, Any, Callable
import time
import logging
import json
import kombu  # type: ignore
import socket

log = logging.getLogger(__name__)


class CallbackData:
    def __init__(self, event_type: str, payload: Dict[str, Any]):
        self.event_type = event_type
        self.payload = payload

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CallbackData):
            return False
        return self.event_type == other.event_type \
            and self.payload == other.payload

    def __repr__(self) -> str:
        return "<CallbackData({0}, {1})".format(self.event_type,
                                                self.payload)


OpenstackNotifierCallback = Optional[Callable[[CallbackData], None]]


class OpenstackNotifier():
    def __init__(self,
                 url: str,
                 callback: OpenstackNotifierCallback = None,
                 neutron_exchange: str = "neutron",
                 neutron_queue: str = "notifications.neutron",
                 neutron_routing_key: str = "notifications.info",
                 nova_exchange: str = "nova",
                 nova_queue: str = "notifications.info",
                 nova_routing_key: str = "notifications.info",
                 min_timestamp: Optional[float] = None,
                 ):
        self.url: str = url
        self.callback = callback
        if min_timestamp is None:
            self.min_timestamp = time.mktime(time.gmtime())
        else:
            self.min_timestamp = min_timestamp
        self.neutron_exchange = neutron_exchange
        self.neutron_queue = neutron_queue
        self.neutron_routing_key = neutron_routing_key
        self.nova_exchange = nova_exchange
        self.nova_queue = nova_queue
        self.nova_routing_key = nova_routing_key
        super().__init__()

        self.rabbitmq = kombu.Connection(
            self.url, failover_strategy='round-robin',
            connect_timeout=2,
            hearthbeat=1)
        self.rabbitmq.ensure_connection(max_retries=3)

        self.thread: Optional[Thread] = None
        self.quit_event = Event()

    def rabbitmq_callback(self, body: Dict[str, Any], message: str) -> None:
        try:
            log.debug('received message: %s' % body)
            if "oslo.message" in body:
                body = json.loads(body['oslo.message'])
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
                log.debug('calling callback (callback_data)')
                self.callback(callback_data)
        except Exception:
            log.exception('Error while parsing message %s' % body)

    def start(self) -> None:
        if self.thread is not None and self.thread.isAlive():
            return
        self.thread = Thread(target=self.run)
        self.thread.start()

    def run(self) -> None:
        try:
            log.debug('start listening to exchange neutron,'
                      'queue notifications.neutron, %s' %
                      self.url)
            neutron_ex = kombu.Exchange(self.neutron_exchange,
                                        type='topic', durable=False)
            neutron_q = kombu.Queue(self.neutron_queue,
                                    exchange=neutron_ex,
                                    routing_key=self.neutron_routing_key,
                                    durable=False)
            nova_ex = kombu.Exchange(self.nova_exchange,
                                     type='topic', durable=False)
            nova_q = kombu.Queue(self.nova_queue,
                                 exchange=nova_ex,
                                 routing_key=self.nova_routing_key,
                                 durable=False)
            self.minimum_timestamp = time.mktime(time.gmtime())

            with self.rabbitmq.channel() as neutron_c, \
                    self.rabbitmq.channel() as nova_c:
                with kombu.utils.compat.nested(
                        kombu.Consumer(neutron_c, neutron_q,
                                       callbacks=[self.rabbitmq_callback]),
                        kombu.Consumer(nova_c, nova_q,
                                       callbacks=[self.rabbitmq_callback])):
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

    def alive(self) -> bool:
        return self.thread is not None and self.thread.isAlive()

    def stop(self) -> None:
        self.quit_event.set()
        if self.thread is not None and self.thread.isAlive():
            self.thread.join()
        self.thread = None
        self.quit_event.clear()
