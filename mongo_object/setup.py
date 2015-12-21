#!/usr/bin/env python
from distutils.core import setup

setup(
    name='mongo_object',
    version='1.0.4',
    author='Damien Tramblay',
    author_email='dt@themetricsfactory.com',
    py_modules=['mongo_object'],
    install_requires=['pymongo', 'simplejson']
)
