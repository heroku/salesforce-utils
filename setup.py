#!/usr/bin/env python

import os
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

packages = [
    'salesforce_utils',
]

requires = [
    'enum>=0.4.4',
    'requests>=2.2.1',
    'salesforce-python-toolkit>=0.1.3',
    'salesforce-oauth-request>=1.0.1',
    'salesforce-bulk>=1.0.0',
]

with open('README.md') as f:
    readme = f.read()
with open('LICENSE') as f:
    license = f.read()

setup(
    name='salesforce-utils',
    version='1.0.1',
    description='Utilities for using the Salesforce.com SOAP API and loading test data.',
    long_description=readme,
    author='Scott Persinger',
    author_email='scottp@heroku.com',
    url='https://github.com/heroku/salesforce-utils',
    packages=packages,
    package_data={'': ['LICENSE']},
    include_package_data=True,
    install_requires=requires,
    license=license,
    zip_safe=False,
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ),
)
