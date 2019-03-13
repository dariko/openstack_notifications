#!/usr/bin/env python

import docker

net = docker.from_env().networks.list(filters={'driver': 'bridge'})[0]
subnet = net.attrs['IPAM']['Config'][0]['Subnet']
print(subnet)
