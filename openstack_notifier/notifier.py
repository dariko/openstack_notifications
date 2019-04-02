from threading import Thread, Event
from typing import Optional, Dict, Any, Callable, List
import time
import logging
import json
import kombu  # type: ignore
import socket

log = logging.getLogger(__name__)


class CallbackData:
    def __init__(self,
                 event_type,         # type: str
                 payload             # type: Dict[str, Any]
                 ):
        self.event_type = event_type
        self.payload = payload

    def __eq__(self,
               other,  # type: object
               ):  # type: (...) -> bool
        if not isinstance(other, CallbackData):
            return False
        return self.event_type == other.event_type \
            and self.payload == other.payload

    def __repr__(self):  # type: (...) -> str
        return "<CallbackData({0}, {1})".format(self.event_type,
                                                self.payload)


class QueueConfig:
    def __init__(self,
                 exchange,     # type: str
                 queue,        # type: str
                 routing_key,  # type: str
                 ):
        self.exchange = exchange
        self.queue = queue
        self.routing_key = routing_key

    def __repr__(self):  # type: () -> str
        return("QueueConfig<exchange=%s queue=%s routing_key=%s>" %
               (self.exchange, self.queue, self.routing_key))

    def __eq__(self, other):  # type: (Any) -> bool
        if not isinstance(other, QueueConfig):
            return False
        return self.exchange == other.exchange \
            and self.queue == other.queue \
            and self.routing_key == other.routing_key


OpenstackNotifierCallback = Optional[Callable[[CallbackData], None]]


class OpenstackNotifier(object):
    def __init__(self,
                 url,                   # type: str
                 callback=None,         # type: OpenstackNotifierCallback
                 queue_configs=None,    # type: Optional[List[QueueConfig]]
                 min_timestamp=None,  # type: Optional[float]
                 ):
        self.url = url
        self.callback = callback
        if min_timestamp is None:
            self.min_timestamp = 0
        else:
            self.min_timestamp = min_timestamp
        if queue_configs is None:
            self.queue_configs = [
                QueueConfig(exchange='neutron',
                            queue='notifications.neutron',
                            routing_key='notifications.info'),
                QueueConfig(exchange='nova',
                            queue='notifications.info',
                            routing_key='notifications.info'),
                ]
        else:
            self.queue_configs = queue_configs
        super(OpenstackNotifier, self).__init__()

        self.thread = None  # type: Optional[Thread]
        self.quit_event = Event()

    def rabbitmq_callback(self,
                          body,  # type: Dict[str, Any]
                          message  # type: str
                          ):  # type: (...) -> None
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
                log.debug('old message, skipping: %s, min_timestamp: %s'
                          % (body, self.min_timestamp))
                return

            if self.callback is not None:
                payload = body.get('payload', {})
                callback_data = CallbackData(event_type=event_type,
                                             payload=payload)
                log.debug('calling callback (%s)' % callback_data)
                self.callback(callback_data)
        except Exception:
            log.exception('Error while parsing message %s' % body)

    def start(self):  # type: () -> None
        if self.thread is not None and self.thread.isAlive():
            return
        self.thread = Thread(target=self.run)
        self.thread.start()

    def run(self):  # type: () -> None
        try:
            rabbitmq = None
            channel = None
            consumer = None
            rabbitmq = kombu.Connection(
                self.url, failover_strategy='round-robin',
                connect_timeout=2, hearthbeat=1)
            rabbitmq.ensure_connection(max_retries=3)
            log.debug('start listening for notifications on queues %s' %
                      self.queue_configs)

            channel = rabbitmq.channel()
            consumer = kombu.Consumer(channel,
                                      callbacks=[self.rabbitmq_callback])

            for q in self.queue_configs:
                exchange = kombu.Exchange(q.exchange,
                                          type='topic',
                                          durable=False)
                q = kombu.Queue(q.queue, exchange=exchange,
                                routing_key=q.routing_key, durable=False,
                                no_ack=True)
                consumer.add_queue(q)

            consumer.consume()
            while not self.quit_event.is_set():
                while not self.quit_event.wait(timeout=1):
                    try:
                        while not self.quit_event.is_set():
                            rabbitmq.drain_events(timeout=1)
                    except socket.timeout:
                        rabbitmq.heartbeat_check()
        except Exception as e:
            log.exception('error in OpenstackManager: %s' % e)
        finally:
            if consumer is not None:
                consumer.cancel()
            if channel is not None:
                channel.close()
            if rabbitmq is not None:
                rabbitmq.release()

    def alive(self):  # type: () -> bool
        return self.thread is not None and self.thread.isAlive()

    def stop(self):  # type: () -> None
        self.quit_event.set()
        if self.thread is not None and self.thread.isAlive():
            self.thread.join()
        self.thread = None
        self.quit_event.clear()
