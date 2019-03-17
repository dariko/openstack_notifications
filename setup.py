#!/usr/bin/env python

from setuptools import setup

setup(name='openstack_notifier',
      version='0.1.0',
      packages=['openstack_notifier'],
      install_requires=['kombu==4.4.0'],
      entry_points={
        'console_scripts': [
            'openstack_notifier = openstack_notifier.tool:openstack_notifier_tool',
        ]},
      package_data={"openstack_notifier": ["py.typed"]},
      )
