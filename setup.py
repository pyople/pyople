#!/usr/bin/env python

import os
import sys

import pyople

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

try:
    import multiprocessing
except ImportError:
    pass

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

packages = [
    'pyople',
    'pyople/tests'
]

requires = [
    'hiredis>=0.1.0',
    'redis>=2.4.11',
    'nose>=1.0'
]

setup(
    name='pyople',
    version=pyople.__version__,
    description='Easy pipeline processing in python',
    long_description=open('README.rst').read() + '\n\n' +
                     open('HISTORY.rst').read(),
    author='David Arazo, Guillem Serra',
    author_email='aquell.gat@gmail.com, serra.guillem@gmail.com',
    url='http://github.com/mabuse/pyople',
    packages=packages,
    package_data={'': ['LICENSE', 'NOTICE'], 'requests': ['*.pem']},
    package_dir={'pyople': 'pyople'},
    include_package_data=True,
    install_requires=requires,
    test_suite='nose.collector',
    license=open('LICENSE').read(),
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Clustering',
        'Topic :: System :: Hardware :: Symmetric Multi-processing'
    ),
)

del os.environ['PYTHONDONTWRITEBYTECODE']
