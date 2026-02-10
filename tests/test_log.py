#!/usr/bin/env python

import confluent_kafka
import logging
import pytest
import time


class CountingFilter(logging.Filter):
    def __init__(self, name):
        self.name = name
        self.cnt = 0

    def filter(self, record):
        print(self.name, record.getMessage())
        self.cnt += 1
        print(record)


def test_logging_consumer():
    """ Tests that logging works """

    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    f = CountingFilter('consumer')
    logger.addFilter(f)

    kc = confluent_kafka.Consumer({'group.id': 'test',
                                   'debug': 'all'},
                                  logger=logger)
    while f.cnt == 0:
        kc.poll(timeout=0.5)

    print('%s: %d log messages seen' % (f.name, f.cnt))

    kc.close()


def test_logging_producer():
    """ Tests that logging works """

    logger = logging.getLogger('producer')
    logger.setLevel(logging.DEBUG)
    f = CountingFilter('producer')
    logger.addFilter(f)

    p = confluent_kafka.Producer({'debug': 'all'}, logger=logger)

    while f.cnt == 0:
        p.poll(timeout=0.5)

    print('%s: %d log messages seen' % (f.name, f.cnt))


def test_logging_constructor():
    """ Verify different forms of constructors """

    for how in ['dict', 'dict+kwarg', 'kwarg']:
        logger = logging.getLogger('producer: ' + how)
        logger.setLevel(logging.DEBUG)
        f = CountingFilter(logger.name)
        logger.addFilter(f)

        if how == 'dict':
            p = confluent_kafka.Producer({'debug': 'all', 'logger': logger})
        elif how == 'dict+kwarg':
            p = confluent_kafka.Producer({'debug': 'all'}, logger=logger)
        elif how == 'kwarg':
            conf = {'debug': 'all', 'logger': logger}
            p = confluent_kafka.Producer(**conf)
        else:
            raise RuntimeError('Not reached')

        print('Test %s with %s' % (p, how))

        while f.cnt == 0:
            p.poll(timeout=0.5)

        print('%s: %s: %d log messages seen' % (how, f.name, f.cnt))


class RaisingLogger(object):
    def __init__(self):
        self.raised = False

    def log(self, *args):
        if self.raised:
            return

        self.raised = True
        raise RuntimeError("log callback failure")


def test_logging_callback_exception_is_propagated():
    logger = RaisingLogger()
    producer = confluent_kafka.Producer({'debug': 'all'}, logger=logger)

    with pytest.raises(RuntimeError) as ex:
        deadline = time.time() + 10.0
        while time.time() < deadline:
            producer.poll(timeout=0.1)
        pytest.fail("log callback was not triggered within timeout")

    assert ex.match("log callback failure")
