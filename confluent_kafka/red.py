# Keep the raw C extension types private to this module; package-level
# Producer/Consumer are replaced with RED wrappers below.
import logging

from .cimpl import CConsumer as _CConsumer
from .cimpl import CProducer as _CProducer
from .red_eds import resolve_bootstrap
from .red_metrics import MetricsSender

_LOGGER = logging.getLogger(__name__)


def _build_conf(conf, kwargs):
    if conf is None:
        if not kwargs:
            raise TypeError("expected configuration dict")
        conf = {}
    elif not isinstance(conf, dict):
        raise TypeError("expected configuration dict")

    merged = dict(conf)
    merged.update(kwargs)
    return merged


class _MetricsClientMixin:
    def _start_metrics(self, conf, client_type):
        self._conf = conf
        self._metrics = MetricsSender(self, conf, client_type, interval=30)
        started = self._metrics.start()
        _LOGGER.debug(
            "metrics sender state: client_type=%s started=%s",
            client_type,
            started,
        )

    def _stop_metrics(self):
        metrics = getattr(self, "_metrics", None)
        if metrics is None:
            return
        metrics.stop()
        self._metrics = None


class Producer(_CProducer, _MetricsClientMixin):
    """Producer client with RED extensions.

    Args:
        conf (dict): Producer configuration.
        **kwargs: Additional producer configuration entries.
    """

    def __init__(self, conf=None, **kwargs):
        merged = _build_conf(conf, kwargs)
        resolved = resolve_bootstrap(merged)
        super(Producer, self).__init__(resolved)
        self._start_metrics(resolved, "Producer")

    def close(self, *args, **kwargs):
        """Close producer and stop background metrics sender.

        Args:
            *args: Positional arguments forwarded to ``flush``.
            **kwargs: Keyword arguments forwarded to ``flush``.
        """
        self._stop_metrics()
        return self.flush(*args, **kwargs)


class Consumer(_CConsumer, _MetricsClientMixin):
    """Consumer client with RED extensions.

    Args:
        conf (dict): Consumer configuration.
        **kwargs: Additional consumer configuration entries.
    """

    def __init__(self, conf=None, **kwargs):
        merged = _build_conf(conf, kwargs)
        resolved = resolve_bootstrap(merged)
        super(Consumer, self).__init__(resolved)
        self._start_metrics(resolved, "Consumer")

    def close(self):
        """Close consumer and stop background metrics sender.

        Returns:
            Any: Return value from ``Consumer.close``.
        """
        self._stop_metrics()
        return super(Consumer, self).close()


# Backward-compatible aliases.
RProducer = Producer
RConsumer = Consumer
