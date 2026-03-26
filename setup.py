#!/usr/bin/env python

import os
import re
from setuptools import setup, find_packages
from distutils.core import Extension
import platform

INSTALL_REQUIRES = [
    'futures;python_version<"3.2"',
    'enum34;python_version<"3.4"',
    'requests',
]

PACKAGE_VERSION = os.environ.get('RED_KAFKA_PACKAGE_VERSION', '1.3.0')


def package_version_hex(version):
    match = re.match(r'^(\d+)\.(\d+)(?:\.(\d+))?', version)
    if match is None:
        raise ValueError('invalid package version: %s' % version)

    major = int(match.group(1))
    minor = int(match.group(2))
    patch = int(match.group(3) or 0)

    if major > 255 or minor > 255 or patch > 255:
        raise ValueError('package version out of range: %s' % version)

    return '0x%02x%02x%02x00' % (major, minor, patch)


PACKAGE_VERSION_HEX = package_version_hex(PACKAGE_VERSION)

AVRO_REQUIRES = [
    'fastavro',
    'requests',
    'avro;python_version<"3.0"',
    'avro-python3;python_version>"3.0"'
]

TEST_REQUIRES = [
    'pytest==4.6.4;python_version<"3.0"',
    'pytest;python_version>="3.0"',
    'pytest-timeout',
    'flake8'
]

# On Un*x the library is linked as -lrdkafka,
# while on windows we need the full librdkafka name.
if platform.system() == 'Windows':
    librdkafka_libname = 'librdkafka'
else:
    librdkafka_libname = 'rdkafka'

module = Extension('confluent_kafka.cimpl',
                   define_macros=[
                       ('CFL_PY_VERSION_STR', '"%s"' % PACKAGE_VERSION),
                       ('CFL_PY_VERSION_HEX', PACKAGE_VERSION_HEX),
                   ],
                   libraries=[librdkafka_libname],
                   sources=['confluent_kafka/src/confluent_kafka.c',
                            'confluent_kafka/src/CProducer.c',
                            'confluent_kafka/src/CConsumer.c',
                            'confluent_kafka/src/Metadata.c',
                            'confluent_kafka/src/AdminTypes.c',
                            'confluent_kafka/src/Admin.c'])


def get_install_requirements(path):
    content = open(os.path.join(os.path.dirname(__file__), path)).read()
    return [
        req
        for req in content.split("\n")
        if req != '' and not req.startswith('#')
    ]


setup(name='red-kafka',
      version=PACKAGE_VERSION,
      description='Red Kafka Python client for Apache Kafka',
      author='Confluent Inc',
      author_email='support@confluent.io',
      url='https://github.com/confluentinc/confluent-kafka-python',
      ext_modules=[module],
      packages=find_packages(exclude=("tests", "tests.*")),
      exclude_package_data={'': ['__pycache__/*', '*.py[cod]']},
      data_files=[('', ['LICENSE.txt'])],
      python_requires='>=3.10',
      install_requires=INSTALL_REQUIRES,
      extras_require={
          'avro': AVRO_REQUIRES,
          'dev': TEST_REQUIRES + AVRO_REQUIRES
      })
