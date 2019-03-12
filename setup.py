#!/usr/bin/env python

from setuptools import setup

setup(name='openstack_events',
      version='0.1.0',
      packages=['openstack_events'],
      install_requires=['openstacksdk==0.17.2', 'kombu==4.4.0'],
      )
