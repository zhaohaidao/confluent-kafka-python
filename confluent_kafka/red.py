from .cimpl import Consumer as _Consumer
from .cimpl import Producer as _Producer
from .red_eds import resolve_eds_bootstrap
from .red_metrics import MetricsSender


def _merge_conf(conf, kwargs):
    if conf is None:
        conf = {}
    if not isinstance(conf, dict):
        raise TypeError("conf must be a dict")
    merged = dict(conf)
    merged.update(kwargs)
    return merged


class _BaseClient:
    def __init__(self, client, conf, client_type):
        self._client = client
        self._conf = conf
        self._metrics = MetricsSender(client, conf, client_type)
        self._metrics.start()

    def __getattr__(self, name):
        return getattr(self._client, name)

    def _stop_metrics(self):
        if self._metrics is None:
            return
        self._metrics.stop()
        self._metrics = None


class RProducer(_BaseClient):
    def __init__(self, conf=None, **kwargs):
        merged = _merge_conf(conf, kwargs)
        resolved = resolve_eds_bootstrap(merged)
        client = _Producer(resolved)
        super().__init__(client, resolved, "Producer")

    def close(self, *args, **kwargs):
        self._stop_metrics()
        return self._client.flush(*args, **kwargs)


class RConsumer(_BaseClient):
    def __init__(self, conf=None, **kwargs):
        merged = _merge_conf(conf, kwargs)
        resolved = resolve_eds_bootstrap(merged)
        client = _Consumer(resolved)
        super().__init__(client, resolved, "Consumer")

    def close(self):
        self._stop_metrics()
        return self._client.close()
