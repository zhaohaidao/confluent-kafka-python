import os

discovery = None
_IMPORT_ERROR = None

EDS_BOOTSTRAP_PREFIX = "eds://"
BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"
SERVICE_NAME_CONFIG = "kafka.service.name"
ORIGINAL_BOOTSTRAP_CONFIG = "original.metadata.broker.list"
_REQUIRED_ENV_VARS = (
    "XHS_ENV",
    "XHS_SERVICE",
    "XHS_REGION",
    "XHS_ZONE",
    "EDS_HTTP_HOST",
)


class EdsResolveError(RuntimeError):
    pass


def resolve_eds_bootstrap(conf):
    if conf is None:
        return conf
    if not isinstance(conf, dict):
        raise TypeError("conf must be a dict")

    _validate_eds_config(conf)
    service_name = _extract_service_name(conf)
    if not service_name:
        return conf

    _validate_env()
    client = _create_eds_client()
    instances = client.get_instances(service_name)
    addresses = _normalize_addresses(instances)
    if not addresses:
        raise EdsResolveError(
            "eds lookup returned no instances for service: %s" % service_name
        )

    resolved = dict(conf)
    resolved.pop(SERVICE_NAME_CONFIG, None)
    resolved[ORIGINAL_BOOTSTRAP_CONFIG] = EDS_BOOTSTRAP_PREFIX + service_name
    resolved[BOOTSTRAP_SERVERS_CONFIG] = ",".join(sorted(set(addresses)))
    return resolved


def _extract_service_name(conf):
    service_name = conf.get(SERVICE_NAME_CONFIG, "")
    if isinstance(service_name, str):
        service_name = service_name.strip()
    else:
        service_name = ""

    bootstrap = conf.get(BOOTSTRAP_SERVERS_CONFIG)
    bootstrap_name = _extract_eds_from_bootstrap(bootstrap)
    if bootstrap_name:
        return bootstrap_name

    if service_name.startswith(EDS_BOOTSTRAP_PREFIX):
        return service_name[len(EDS_BOOTSTRAP_PREFIX) :].strip()
    return service_name


def _validate_eds_config(conf):
    bootstrap = conf.get(BOOTSTRAP_SERVERS_CONFIG)
    if _bootstrap_has_eds_prefix(bootstrap) and not _extract_eds_from_bootstrap(
        bootstrap
    ):
        raise EdsResolveError("eds service name is empty")
    service_name = conf.get(SERVICE_NAME_CONFIG)
    if isinstance(service_name, str) and service_name.strip() == EDS_BOOTSTRAP_PREFIX:
        raise EdsResolveError("eds service name is empty")


def _extract_eds_from_bootstrap(bootstrap):
    if isinstance(bootstrap, str):
        return _strip_eds_prefix(bootstrap)
    if isinstance(bootstrap, (list, tuple)) and bootstrap:
        first = bootstrap[0]
        if isinstance(first, str):
            return _strip_eds_prefix(first)
    return ""


def _bootstrap_has_eds_prefix(bootstrap):
    if isinstance(bootstrap, str):
        return bootstrap.strip().startswith(EDS_BOOTSTRAP_PREFIX)
    if isinstance(bootstrap, (list, tuple)) and bootstrap:
        first = bootstrap[0]
        if isinstance(first, str):
            return first.strip().startswith(EDS_BOOTSTRAP_PREFIX)
    return False


def _strip_eds_prefix(value):
    if not value:
        return ""
    value = value.strip()
    if not value.startswith(EDS_BOOTSTRAP_PREFIX):
        return ""
    return value[len(EDS_BOOTSTRAP_PREFIX) :].strip()


def _validate_env():
    missing = [key for key in _REQUIRED_ENV_VARS if not os.getenv(key)]
    if missing:
        raise EdsResolveError(
            "missing eds environment variables: %s" % ",".join(missing)
        )


def _create_eds_client():
    module = _import_redinfra()
    if module is None:
        raise EdsResolveError(
            "redinfra.discovery import failed: %s" % _IMPORT_ERROR
        )
    return module.client.EdsClient()


def _import_redinfra():
    global discovery, _IMPORT_ERROR
    if discovery is not None:
        return discovery
    try:
        from redinfra import discovery as _discovery
    except Exception as exc:
        _IMPORT_ERROR = exc
        return None
    discovery = _discovery
    _IMPORT_ERROR = None
    return discovery


def _normalize_addresses(instances):
    if not instances:
        return []
    addresses = []
    for instance in instances:
        addr = _extract_address(instance)
        if addr:
            addresses.append(addr)
    return addresses


def _extract_address(instance):
    if instance is None:
        return ""
    if isinstance(instance, str):
        return instance.strip()
    if isinstance(instance, dict):
        addr = instance.get("address") or instance.get("addr")
        if addr:
            return str(addr).strip()
        host = instance.get("host") or instance.get("ip") or instance.get("hostname")
        port = instance.get("port")
        return _format_host_port(host, port)

    addr = getattr(instance, "address", None)
    if addr:
        return str(addr).strip()
    host = (
        getattr(instance, "host", None)
        or getattr(instance, "ip", None)
        or getattr(instance, "hostname", None)
    )
    port = getattr(instance, "port", None)
    return _format_host_port(host, port)


def _format_host_port(host, port):
    if not host or port is None:
        return ""
    return "%s:%s" % (host, port)
