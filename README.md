# Openstack Notifier

## installation

This module is compatible with `python >= 3.6`.     
It can be installed using pip:

`````
pip install git+https://gitlab.prod.postecom.local/infrastrutture/openstack_notifier/
`````

## usage

This module abstracts the openstack (nova and neutron) rabbitmq
notifications to a callback call.

The `OpenstackNotifier` class can be instantiated as:
`````
OpenstackNotifier(self,
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
`````

`url` is the rabbitmq url as described [here](http://docs.celeryproject.org/projects/kombu/en/latest/userguide/connections.html#urls) (kombu documentation).

The `start()` and `stop()` methods can be used to start and stop the
queue monitoring thread, and the `alive()` method will return `True` if
the monitoring thread is alive.

`callback` will be called for every received notification with a
CallbackData as parameter.
`````
CallbackData(self, event_type: str, payload: Dict[str, Any]):
`````

If `min_timestamp` it will act as a notification filter.



## command line tool

`````
usage: openstack_notifier [-h] [--neutron_exchange NEUTRON_EXCHANGE]
                          [--neutron_queue NEUTRON_QUEUE]
                          [--neutron_routing_key NEUTRON_ROUTING_KEY]
                          [--nova_exchange NOVA_EXCHANGE]
                          [--nova_queue NOVA_QUEUE]
                          [--nova_routing_key NOVA_ROUTING_KEY]
                          [--min_timestamp MIN_TIMESTAMP] [--debug]
                          --rabbitmq_url RABBITMQ_URL

openstack notifications monitor

optional arguments:
  -h, --help            show this help message and exit
  --neutron_exchange NEUTRON_EXCHANGE
  --neutron_queue NEUTRON_QUEUE
  --neutron_routing_key NEUTRON_ROUTING_KEY
  --nova_exchange NOVA_EXCHANGE
  --nova_queue NOVA_QUEUE
  --nova_routing_key NOVA_ROUTING_KEY
  --min_timestamp MIN_TIMESTAMP
  --debug
  --rabbitmq_url RABBITMQ_URL
`````

example:

`````
$ openstack_notifier  --rabbitmq_url amqp://openstack:password@rabbit1
INFO:openstack_notifier.tool:start monitoring
INFO:openstack_notifier.tool:<CallbackData(subnet.delete.start, {'subnet_id': 'd22e9750-f4a4-41b8-993b-6fa7db143c3d'})
INFO:openstack_notifier.tool:<CallbackData(subnet.delete.start, {'subnet_id': 'd22e9750-f4a4-41b8-993b-6fa7db143c3d'})
INFO:openstack_notifier.tool:<CallbackData(subnet.delete.end, {'subnet_id': 'd22e9750-f4a4-41b8-993b-6fa7db143c3d', 'subnet': {'description': '', 'tags': [], 'updated_at': '2019-03-15T08:32:59Z', 'ipv6_ra_mode': None, 'allocation_pools': [{'start': '10.70.156.162', 'end': '10.70.156.190'}], 'host_routes': [{'nexthop': '0.0.0.0', 'destination': '169.254.0.0/16'}], 'revision_number': 0, 'ipv6_address_mode': None, 'cidr': '10.70.156.160/27', 'id': 'd22e9750-f4a4-41b8-993b-6fa7db143c3d', 'subnetpool_id': 'e7f42f51-ed97-459f-9e8d-14dfeb98d20d', 'service_types': [], 'name': 'pec-collaudo-static.subnet', 'enable_dhcp': False, 'segment_id': None, 'network_id': '8255253d-d6b9-4d3e-8032-4440998dc77f', 'tenant_id': 'a259eaeebee34985b4728bfb111a62ad', 'created_at': '2019-03-15T08:32:59Z', 'dns_nameservers': ['169.254.169.239'], 'gateway_ip': '10.70.156.161', 'ip_version': 4, 'shared': False, 'project_id': 'a259eaeebee34985b4728bfb111a62ad'}})
INFO:openstack_notifier.tool:<CallbackData(subnet.delete.end, {'subnet_id': 'd22e9750-f4a4-41b8-993b-6fa7db143c3d', 'subnet': {'description': '', 'tags': [], 'updated_at': '2019-03-15T08:32:59Z', 'ipv6_ra_mode': None, 'allocation_pools': [{'start': '10.70.156.162', 'end': '10.70.156.190'}], 'host_routes': [{'nexthop': '0.0.0.0', 'destination': '169.254.0.0/16'}], 'revision_number': 0, 'ipv6_address_mode': None, 'cidr': '10.70.156.160/27', 'id': 'd22e9750-f4a4-41b8-993b-6fa7db143c3d', 'subnetpool_id': 'e7f42f51-ed97-459f-9e8d-14dfeb98d20d', 'service_types': [], 'name': 'pec-collaudo-static.subnet', 'enable_dhcp': False, 'segment_id': None, 'network_id': '8255253d-d6b9-4d3e-8032-4440998dc77f', 'tenant_id': 'a259eaeebee34985b4728bfb111a62ad', 'created_at': '2019-03-15T08:32:59Z', 'dns_nameservers': ['169.254.169.239'], 'gateway_ip': '10.70.156.161', 'ip_version': 4, 'shared': False, 'project_id': 'a259eaeebee34985b4728bfb111a62ad'}})
INFO:openstack_notifier.tool:<CallbackData(network.delete.start, {'network_id': '8255253d-d6b9-4d3e-8032-4440998dc77f'})
INFO:openstack_notifier.tool:<CallbackData(network.delete.start, {'network_id': '8255253d-d6b9-4d3e-8032-4440998dc77f'})
INFO:openstack_notifier.tool:<CallbackData(router.delete.start, {'router_id': '82cc7bb9-e048-4274-b159-0c815297e8cb'})
INFO:openstack_notifier.tool:<CallbackData(router.delete.start, {'router_id': '82cc7bb9-e048-4274-b159-0c815297e8cb'})
`````

# examples

Usage examples can be found in [tool.py](openstack_notifier/tool.py) and
in the [tests](tests/).
