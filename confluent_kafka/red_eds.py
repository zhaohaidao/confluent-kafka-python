import json
import logging
import os
import threading
from urllib.parse import quote, urlsplit, urlunsplit

_LOGGER = logging.getLogger(__name__)

EDS_BOOTSTRAP_PREFIX = "eds://"
CLUSTER_BOOTSTRAP_PREFIX = "cluster://"
BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"
SERVICE_NAME_CONFIG = "kafka.service.name"
CLUSTER_NAME_CONFIG = "kafka.cluster.name"
ORIGINAL_BOOTSTRAP_CONFIG = "original.metadata.broker.list"
SECURITY_PROTOCOL_CONFIG = "security.protocol"
SECURITY_PROTOCOL_ENV = "kafka_security_protocol"
SASL_JAAS_CONFIG = "sasl.jaas.config"
SASL_JAAS_CONFIG_ENV = "kafka_sasl_jaas_config"
EDS_SECURITY_SERVICE_NAME_SUFFIX = "sasl"
ENV_CONFIG = "env.config"
KMETA_URL_ENV = "KMETA_URL"
KMETA_CLUSTER_API = "/api/kmeta/cluster/"
KMETA_SECURITY_BOOTSTRAP_API = "/api/kmeta/cluster/security-bootstrap/"
KMETA_REQUEST_TIMEOUT_SECONDS = 15
_REQUIRED_ENV_VARS = ("EDS_HTTP_HOST",)
_REDACTED_CONFIG_VALUE = "[redacted]"
_SENSITIVE_CONFIG_KEY_TOKENS = (
    "password",
    "jaas.config",
    "secret",
    "token",
)
_OBSERVABILITY_CONFIG_KEYS = (
    BOOTSTRAP_SERVERS_CONFIG,
    SERVICE_NAME_CONFIG,
    CLUSTER_NAME_CONFIG,
    ORIGINAL_BOOTSTRAP_CONFIG,
    SECURITY_PROTOCOL_CONFIG,
    ENV_CONFIG,
    "client.id",
    "group.id",
    "metrics.collect.url",
    SASL_JAAS_CONFIG,
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


class _BuiltinEdsClient:
    def __init__(self, timeout=5):
        self._eds_http_host = os.getenv("EDS_HTTP_HOST", "").strip()
        self._client_name = os.getenv("XHS_SERVICE", "").strip()
        self._locality = os.getenv("XHS_ZONE", "").strip()
        self._timeout = timeout
        self._lock = threading.Lock()
        self._instances_cache = {}
        self._service_version = {}

    def get_instances(self, service_name):
        if not service_name:
            return []

        params = {
            "serviceName": service_name,
            "apiVersion": "v1",
        }
        if self._client_name:
            params["clientName"] = self._client_name
        if self._locality:
            params["locality"] = self._locality

        with self._lock:
            current_version = self._service_version.get(service_name)
        if current_version:
            params["version"] = current_version

        _LOGGER.info(
            "eds endpoint request: service=%s host=%s client_name_set=%s locality_set=%s version_cached=%s",
            service_name,
            self._eds_http_host,
            bool(self._client_name),
            bool(self._locality),
            bool(current_version),
        )
        response = _http_get(
            "http://%s/endpoints" % self._eds_http_host,
            timeout=self._timeout,
            params=params,
        )
        status_code = getattr(response, "status_code", None)
        if status_code != 200:
            raise EdsResolveError(
                "eds request failed (%s) for service: %s"
                % (status_code, service_name)
            )

        try:
            payload = response.json()
        except Exception as exc:
            raise EdsResolveError(
                "eds response decode failed for service %s: %s"
                % (service_name, exc)
            )

        if not isinstance(payload, dict):
            raise EdsResolveError(
                "eds response format invalid for service: %s" % service_name
            )

        code = payload.get("code")
        if code == 1:
            # EDS uses code=1 to mean "not modified"; reuse the last endpoints
            # for this service when the server answers with only a version hit.
            with self._lock:
                cached = list(self._instances_cache.get(service_name, []))
            _LOGGER.info(
                "eds endpoint cache reused: service=%s endpoint_count=%d",
                service_name,
                len(cached),
            )
            return cached
        if code != 0:
            raise EdsResolveError(
                "eds response code %s for service: %s" % (code, service_name)
            )

        data = payload.get("data")
        if not isinstance(data, dict):
            raise EdsResolveError(
                "eds response missing data for service: %s" % service_name
            )

        endpoints = data.get("endpoints") or []
        version = data.get("version")

        with self._lock:
            self._instances_cache[service_name] = list(endpoints)
            if version:
                self._service_version[service_name] = version

        _LOGGER.info(
            "eds endpoint response accepted: service=%s endpoint_count=%d version_set=%s",
            service_name,
            len(endpoints),
            bool(version),
        )
        return list(endpoints)


def resolve_bootstrap(conf):
    if conf is None:
        return conf
    if not isinstance(conf, dict):
        raise TypeError("conf must be a dict")

    _validate_eds_config(conf)
    _validate_cluster_config(conf)

    route = _select_bootstrap_source(conf)
    if route is None:
        _LOGGER.debug(
            "bootstrap resolution skipped: config=%s",
            _config_snapshot(conf),
        )
        return conf

    route_type, logical_name, original_bootstrap = route
    security_enabled, security_reason, security_protocol = _security_state(conf)
    _LOGGER.info(
        "bootstrap resolution started: route=%s logical=%s original=%s "
        "security_enabled=%s security_reason=%s security_protocol=%s config=%s",
        route_type,
        logical_name,
        original_bootstrap,
        security_enabled,
        security_reason,
        security_protocol,
        _config_snapshot(conf),
    )

    try:
        if route_type == "eds":
            resolved_logical_name = _resolve_eds_service_name(logical_name, conf)
            addresses = _resolve_eds_addresses(resolved_logical_name)
        else:
            resolved_logical_name = logical_name
            addresses = _resolve_kmeta_addresses(logical_name, conf)
    except Exception:
        _LOGGER.exception(
            "bootstrap resolution failed: route=%s logical=%s original=%s security_enabled=%s security_reason=%s",
            route_type,
            logical_name,
            original_bootstrap,
            security_enabled,
            security_reason,
        )
        raise

    if not addresses:
        _LOGGER.error(
            "bootstrap resolution returned no addresses: route=%s logical=%s "
            "resolved_logical=%s original=%s security_enabled=%s security_reason=%s",
            route_type,
            logical_name,
            resolved_logical_name,
            original_bootstrap,
            security_enabled,
            security_reason,
        )
        if route_type == "eds":
            raise EdsResolveError(
                "eds lookup returned no instances for service: %s"
                % resolved_logical_name
            )
        raise KmetaResolveError(
            "kmeta lookup returned no bootstrap servers for cluster: %s"
            % logical_name
        )

    resolved = dict(conf)
    resolved.pop(SERVICE_NAME_CONFIG, None)
    resolved.pop(CLUSTER_NAME_CONFIG, None)
    # Preserve the logical bootstrap for metrics/debugging after librdkafka sees
    # only physical broker addresses.
    resolved[ORIGINAL_BOOTSTRAP_CONFIG] = original_bootstrap
    resolved[BOOTSTRAP_SERVERS_CONFIG] = ",".join(_unique_preserve_order(addresses))
    _LOGGER.info(
        "bootstrap resolution completed: route=%s logical=%s resolved_logical=%s "
        "original=%s address_count=%d security_enabled=%s security_reason=%s",
        route_type,
        logical_name,
        resolved_logical_name,
        original_bootstrap,
        len(_unique_preserve_order(addresses)),
        security_enabled,
        security_reason,
    )
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
    _LOGGER.info("eds bootstrap lookup started: service=%s", service_name)
    client = _create_eds_client()
    instances = client.get_instances(service_name)
    addresses = _normalize_addresses(instances)
    _LOGGER.info(
        "eds bootstrap lookup completed: service=%s address_count=%d",
        service_name,
        len(addresses),
    )
    return addresses


def _resolve_kmeta_addresses(cluster_name, conf):
    kmeta_url = _resolve_kmeta_url(conf)
    security_enabled, security_reason, security_protocol = _security_state(conf)
    # KMeta exposes a different bootstrap endpoint for SASL-enabled clusters;
    # JAAS can come either from client config or the Java-compatible env var.
    api_path = (
        KMETA_SECURITY_BOOTSTRAP_API if security_enabled else KMETA_CLUSTER_API
    )
    url = _join_url(kmeta_url, api_path + quote(cluster_name, safe=""))
    _LOGGER.info(
        "kmeta bootstrap request: cluster=%s endpoint=%s security_enabled=%s security_reason=%s security_protocol=%s",
        cluster_name,
        api_path,
        security_enabled,
        security_reason,
        security_protocol,
    )

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

    addresses = [item.strip() for item in bootstrap.split(",") if item.strip()]
    _LOGGER.info(
        "kmeta bootstrap response accepted: cluster=%s address_count=%d security_enabled=%s",
        cluster_name,
        len(addresses),
        security_enabled,
    )
    return addresses


def _http_get(url, timeout, params=None):
    import requests

    return requests.get(url, timeout=timeout, params=params)


def _extract_bootstrap_from_kmeta_response(payload):
    payload = (payload or "").strip()
    if not payload:
        return ""

    # Historical KMeta deployments returned either plain text, a JSON string,
    # or nested JSON. Accept all three to keep cluster:// resolution compatible.
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
    return _security_state(conf)[0]


def _security_state(conf):
    protocol = _resolve_security_protocol(conf)
    if protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        return True, SECURITY_PROTOCOL_CONFIG, protocol

    return False, "none", protocol


def _resolve_security_protocol(conf):
    protocol = conf.get(SECURITY_PROTOCOL_CONFIG)
    if isinstance(protocol, str):
        return protocol.strip().upper()
    return ""


def _resolve_eds_service_name(service_name, conf):
    if not service_name:
        return service_name
    if not _security_enabled(conf):
        return service_name
    if service_name.lower().endswith(EDS_SECURITY_SERVICE_NAME_SUFFIX):
        return service_name[: -len(EDS_SECURITY_SERVICE_NAME_SUFFIX)] + EDS_SECURITY_SERVICE_NAME_SUFFIX
    return service_name + EDS_SECURITY_SERVICE_NAME_SUFFIX


def _resolve_kmeta_url(conf):
    kmeta_url = os.getenv(KMETA_URL_ENV, "").strip()
    if kmeta_url:
        _LOGGER.info("kmeta url resolved: source=%s", KMETA_URL_ENV)
        return kmeta_url

    env_name = _resolve_kmeta_env(conf)
    if not env_name:
        _LOGGER.error(
            "kmeta env missing: config=%s job_env_set=%s xhs_env_set=%s kmeta_url_set=%s",
            _config_snapshot(conf),
            bool(os.getenv("JOB_ENV", "").strip()),
            bool(os.getenv("XHS_ENV", "").strip()),
            bool(os.getenv(KMETA_URL_ENV, "").strip()),
        )
        raise KmetaResolveError(
            "missing kmeta env, set env.config/JOB_ENV/XHS_ENV or KMETA_URL"
        )

    kmeta_url = _KMETA_URL_BY_ENV.get(env_name)
    if not kmeta_url:
        _LOGGER.error("kmeta env unsupported: env=%s", env_name)
        raise KmetaResolveError("unsupported kmeta env: %s" % env_name)
    _LOGGER.info("kmeta url resolved: source=env env=%s", env_name)
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


def _unique_preserve_order(values):
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


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
        _LOGGER.error("eds env missing: vars=%s", ",".join(missing))
        raise EdsResolveError(
            "missing eds environment variables: %s" % ",".join(missing)
        )


def _create_eds_client():
    return _BuiltinEdsClient()


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


def _config_snapshot(conf):
    snapshot = {}
    for key in _OBSERVABILITY_CONFIG_KEYS:
        if key not in conf:
            continue
        value = conf.get(key)
        if value is None or value == "":
            continue
        snapshot[key] = _safe_config_value(key, value)
    return snapshot


def _safe_config_value(key, value):
    if _is_sensitive_config_key(key):
        return _REDACTED_CONFIG_VALUE
    if key == "metrics.collect.url":
        return _safe_url_value(value)
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value)
    return str(value)


def _is_sensitive_config_key(key):
    normalized = str(key).lower().replace("_", ".")
    if any(token in normalized for token in _SENSITIVE_CONFIG_KEY_TOKENS):
        return True
    return normalized.endswith(".key") or ".key." in normalized


def _safe_url_value(value):
    url = str(value)
    parsed = urlsplit(url)
    if not parsed.scheme and not parsed.netloc:
        parsed = urlsplit("http://%s" % url)
        safe = urlunsplit(("", parsed.netloc, parsed.path, "", ""))
        return safe or str(value).split("?", 1)[0].split("#", 1)[0]
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
