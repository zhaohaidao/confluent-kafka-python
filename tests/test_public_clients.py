import pytest

import confluent_kafka
from confluent_kafka import Consumer, Producer, RConsumer, RProducer
from confluent_kafka.cimpl import CConsumer, CProducer
from confluent_kafka.red import Consumer as RedConsumer
from confluent_kafka.red import Producer as RedProducer


def test_public_clients_use_red_wrappers():
    assert Producer is RedProducer
    assert Consumer is RedConsumer
    assert RProducer is Producer
    assert RConsumer is Consumer
    assert issubclass(Producer, CProducer)
    assert issubclass(Consumer, CConsumer)


def test_c_clients_are_not_exported_from_package_root():
    assert not hasattr(confluent_kafka, "CProducer")
    assert not hasattr(confluent_kafka, "CConsumer")

    with pytest.raises(ImportError):
        exec("from confluent_kafka import CProducer")

    with pytest.raises(ImportError):
        exec("from confluent_kafka import CConsumer")
