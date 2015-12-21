#!/usr/bin/env python
from distutils.core import setup

setup(
    name='webcache2',
    version='2.0.8',
    author='Damien Tramblay',
    author_email='dt@themetricsfactory.com',
    py_modules=['webcache2'],
    requires=['pymongo', 'BeautifulSoup', 'logging'],
)
