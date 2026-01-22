from .cimpl import Consumer as _Consumer
from .cimpl import Producer as _Producer
from .red_eds import resolve_eds_bootstrap


def _merge_conf(conf, kwargs):
    if conf is None:
        conf = {}
    if not isinstance(conf, dict):
        raise TypeError("conf must be a dict")
    merged = dict(conf)
    merged.update(kwargs)
    return merged


class _BaseClient:
    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        return getattr(self._client, name)

class RProducer(_BaseClient):
    def __init__(self, conf=None, **kwargs):
        merged = _merge_conf(conf, kwargs)
        resolved = resolve_eds_bootstrap(merged)
        client = _Producer(resolved)
        super().__init__(client)

    def close(self, *args, **kwargs):
        return self._client.flush(*args, **kwargs)


class RConsumer(_BaseClient):
    def __init__(self, conf=None, **kwargs):
        merged = _merge_conf(conf, kwargs)
        resolved = resolve_eds_bootstrap(merged)
        client = _Consumer(resolved)
        super().__init__(client)

    def close(self):
        return self._client.close()
