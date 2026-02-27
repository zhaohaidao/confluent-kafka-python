import json
import os
from urllib.parse import quote


discovery = None
_IMPORT_ERROR = None

EDS_BOOTSTRAP_PREFIX = "eds://"
CLUSTER_BOOTSTRAP_PREFIX = "cluster://"
BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"
SERVICE_NAME_CONFIG = "kafka.service.name"
CLUSTER_NAME_CONFIG = "kafka.cluster.name"
ORIGINAL_BOOTSTRAP_CONFIG = "original.metadata.broker.list"
SASL_JAAS_CONFIG = "sasl.jaas.config"
SASL_JAAS_CONFIG_ENV = "kafka_sasl_jaas_config"
ENV_CONFIG = "env.config"
KMETA_URL_ENV = "KMETA_URL"
KMETA_CLUSTER_API = "/api/kmeta/cluster/"
KMETA_SECURITY_BOOTSTRAP_API = "/api/kmeta/cluster/security-bootstrap/"
KMETA_REQUEST_TIMEOUT_SECONDS = 15
_REQUIRED_ENV_VARS = (
    "XHS_ENV",
    "XHS_SERVICE",
    "XHS_REGION",
    "XHS_ZONE",
    "EDS_HTTP_HOST",
)
_KMETA_URL_BY_ENV = {
    "PROD_AWSSG": "http://events.int.galapand.com",
    "PROD_AWSSG1": "http://events.int.galapand.com",
    "PROD": "http://events.int.xiaohongshu.com",
    "STAGING": "http://events.int.xiaohongshu.com",
    "SHADOW": "http://events.int.xiaohongshu.com",
    "SIT": "http://events.int.sit.xiaohongshu.com",
    "TEST": "http://events.int.sit.xiaohongshu.com",
    "DEV": "http://events.int.sit.xiaohongshu.com",
    "LOCAL": "http://localhost:8080",
}


class EdsResolveError(RuntimeError):
    pass


class KmetaResolveError(RuntimeError):
    pass


def resolve_bootstrap(conf):
    if conf is None:
        return conf
    if not isinstance(conf, dict):
        raise TypeError("conf must be a dict")

    _validate_eds_config(conf)
    _validate_cluster_config(conf)

    route = _select_bootstrap_source(conf)
    if route is None:
        return conf

    route_type, logical_name, original_bootstrap = route
    if route_type == "eds":
        addresses = _resolve_eds_addresses(logical_name)
    else:
        addresses = _resolve_kmeta_addresses(logical_name, conf)

    if not addresses:
        if route_type == "eds":
            raise EdsResolveError(
                "eds lookup returned no instances for service: %s" % logical_name
            )
        raise KmetaResolveError(
            "kmeta lookup returned no bootstrap servers for cluster: %s"
            % logical_name
        )

    resolved = dict(conf)
    resolved.pop(SERVICE_NAME_CONFIG, None)
    resolved.pop(CLUSTER_NAME_CONFIG, None)
    resolved[ORIGINAL_BOOTSTRAP_CONFIG] = original_bootstrap
    resolved[BOOTSTRAP_SERVERS_CONFIG] = ",".join(sorted(set(addresses)))
    return resolved


def resolve_eds_bootstrap(conf):
    """Backward compatible wrapper."""
    return resolve_bootstrap(conf)


def _select_bootstrap_source(conf):
    bootstrap = conf.get(BOOTSTRAP_SERVERS_CONFIG)
    eds_name = _extract_eds_from_bootstrap(bootstrap)
    if eds_name:
        return "eds", eds_name, EDS_BOOTSTRAP_PREFIX + eds_name

    cluster_name = _extract_cluster_from_bootstrap(bootstrap)
    if cluster_name:
        return "cluster", cluster_name, CLUSTER_BOOTSTRAP_PREFIX + cluster_name

    service_name = _extract_service_name(conf)
    if service_name:
        return "eds", service_name, EDS_BOOTSTRAP_PREFIX + service_name

    cluster_name = _extract_cluster_name(conf)
    if cluster_name:
        return "cluster", cluster_name, CLUSTER_BOOTSTRAP_PREFIX + cluster_name

    return None


def _resolve_eds_addresses(service_name):
    _validate_env()
    client = _create_eds_client()
    instances = client.get_instances(service_name)
    return _normalize_addresses(instances)


def _resolve_kmeta_addresses(cluster_name, conf):
    kmeta_url = _resolve_kmeta_url(conf)
    security_enabled = _security_enabled(conf)
    api_path = (
        KMETA_SECURITY_BOOTSTRAP_API if security_enabled else KMETA_CLUSTER_API
    )
    url = _join_url(kmeta_url, api_path + quote(cluster_name, safe=""))

    response = _http_get(url, timeout=KMETA_REQUEST_TIMEOUT_SECONDS)
    status_code = getattr(response, "status_code", None)
    if status_code != 200:
        raise KmetaResolveError(
            "kmeta request failed (%s) for cluster: %s"
            % (status_code, cluster_name)
        )

    bootstrap = _extract_bootstrap_from_kmeta_response(
        getattr(response, "text", "")
    )
    if not bootstrap:
        raise KmetaResolveError(
            "kmeta response missing bootstrap for cluster: %s" % cluster_name
        )

    return [item.strip() for item in bootstrap.split(",") if item.strip()]


def _http_get(url, timeout):
    import requests

    return requests.get(url, timeout=timeout)


def _extract_bootstrap_from_kmeta_response(payload):
    payload = (payload or "").strip()
    if not payload:
        return ""

    try:
        data = json.loads(payload)
    except Exception:
        return payload

    if isinstance(data, str):
        return data.strip()

    if isinstance(data, dict):
        bootstrap = data.get("bootstrapStr")
        if isinstance(bootstrap, str) and bootstrap.strip():
            return bootstrap.strip()

        nested = data.get("data")
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
        if isinstance(nested, dict):
            nested_bootstrap = nested.get("bootstrapStr") or nested.get("bootstrap")
            if isinstance(nested_bootstrap, str) and nested_bootstrap.strip():
                return nested_bootstrap.strip()

    return payload


def _security_enabled(conf):
    jaas_config = conf.get(SASL_JAAS_CONFIG)
    if jaas_config is not None and str(jaas_config).strip():
        return True
    return bool(os.getenv(SASL_JAAS_CONFIG_ENV, "").strip())


def _resolve_kmeta_url(conf):
    kmeta_url = os.getenv(KMETA_URL_ENV, "").strip()
    if kmeta_url:
        return kmeta_url

    env_name = _resolve_kmeta_env(conf)
    if not env_name:
        raise KmetaResolveError(
            "missing kmeta env, set env.config/JOB_ENV/XHS_ENV or KMETA_URL"
        )

    kmeta_url = _KMETA_URL_BY_ENV.get(env_name)
    if not kmeta_url:
        raise KmetaResolveError("unsupported kmeta env: %s" % env_name)
    return kmeta_url


def _resolve_kmeta_env(conf):
    configured = conf.get(ENV_CONFIG)
    env_name = configured.strip() if isinstance(configured, str) else ""
    if not env_name:
        env_name = os.getenv("JOB_ENV", "").strip()
    if not env_name:
        env_name = os.getenv("XHS_ENV", "").strip()
    if not env_name:
        return ""

    env_name = env_name.upper()
    region = os.getenv("XHS_ZONE", "").strip().upper()
    if region:
        regional_name = "%s_%s" % (env_name, region)
        if regional_name in _KMETA_URL_BY_ENV:
            return regional_name

    return env_name


def _join_url(base_url, path):
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def _extract_service_name(conf):
    service_name = conf.get(SERVICE_NAME_CONFIG, "")
    if isinstance(service_name, str):
        service_name = service_name.strip()
    else:
        service_name = ""

    if service_name.startswith(EDS_BOOTSTRAP_PREFIX):
        return service_name[len(EDS_BOOTSTRAP_PREFIX) :].strip()

    return service_name


def _extract_cluster_name(conf):
    cluster_name = conf.get(CLUSTER_NAME_CONFIG, "")
    if isinstance(cluster_name, str):
        cluster_name = cluster_name.strip()
    else:
        cluster_name = ""

    if cluster_name.startswith(CLUSTER_BOOTSTRAP_PREFIX):
        return cluster_name[len(CLUSTER_BOOTSTRAP_PREFIX) :].strip()

    return cluster_name


def _validate_eds_config(conf):
    bootstrap = conf.get(BOOTSTRAP_SERVERS_CONFIG)
    if _bootstrap_has_eds_prefix(bootstrap) and not _extract_eds_from_bootstrap(
        bootstrap
    ):
        raise EdsResolveError("eds service name is empty")
    service_name = conf.get(SERVICE_NAME_CONFIG)
    if isinstance(service_name, str) and service_name.strip() == EDS_BOOTSTRAP_PREFIX:
        raise EdsResolveError("eds service name is empty")


def _validate_cluster_config(conf):
    bootstrap = conf.get(BOOTSTRAP_SERVERS_CONFIG)
    if _bootstrap_has_cluster_prefix(bootstrap) and not _extract_cluster_from_bootstrap(
        bootstrap
    ):
        raise KmetaResolveError("cluster name is empty")

    cluster_name = conf.get(CLUSTER_NAME_CONFIG)
    if (
        isinstance(cluster_name, str)
        and cluster_name.strip() == CLUSTER_BOOTSTRAP_PREFIX
    ):
        raise KmetaResolveError("cluster name is empty")


def _extract_eds_from_bootstrap(bootstrap):
    if isinstance(bootstrap, str):
        return _strip_eds_prefix(bootstrap)
    if isinstance(bootstrap, (list, tuple)) and bootstrap:
        first = bootstrap[0]
        if isinstance(first, str):
            return _strip_eds_prefix(first)
    return ""


def _extract_cluster_from_bootstrap(bootstrap):
    if isinstance(bootstrap, str):
        return _strip_cluster_prefix(bootstrap)
    if isinstance(bootstrap, (list, tuple)) and bootstrap:
        first = bootstrap[0]
        if isinstance(first, str):
            return _strip_cluster_prefix(first)
    return ""


def _bootstrap_has_eds_prefix(bootstrap):
    if isinstance(bootstrap, str):
        return bootstrap.strip().startswith(EDS_BOOTSTRAP_PREFIX)
    if isinstance(bootstrap, (list, tuple)) and bootstrap:
        first = bootstrap[0]
        if isinstance(first, str):
            return first.strip().startswith(EDS_BOOTSTRAP_PREFIX)
    return False


def _bootstrap_has_cluster_prefix(bootstrap):
    if isinstance(bootstrap, str):
        return bootstrap.strip().startswith(CLUSTER_BOOTSTRAP_PREFIX)
    if isinstance(bootstrap, (list, tuple)) and bootstrap:
        first = bootstrap[0]
        if isinstance(first, str):
            return first.strip().startswith(CLUSTER_BOOTSTRAP_PREFIX)
    return False


def _strip_eds_prefix(value):
    if not value:
        return ""
    value = value.strip()
    if not value.startswith(EDS_BOOTSTRAP_PREFIX):
        return ""
    return value[len(EDS_BOOTSTRAP_PREFIX) :].strip()


def _strip_cluster_prefix(value):
    if not value:
        return ""
    value = value.strip()
    if not value.startswith(CLUSTER_BOOTSTRAP_PREFIX):
        return ""
    return value[len(CLUSTER_BOOTSTRAP_PREFIX) :].strip()


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
