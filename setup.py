#!/usr/bin/env python

from setuptools import setup

setup(name='openstack_events',
      version='0.1.0',
      packages=['openstack_events'],
      install_requires=['kombu==4.4.0'],
      entry_points={
        'console_scripts': [
            'openstack_events_monitor = openstack_events.tool:openstack_events_monitor',
        ]},
      )
