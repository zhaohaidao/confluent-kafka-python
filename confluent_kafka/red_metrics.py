import gzip
import json
import logging
import os
import random
import socket
import threading
import time
import urllib.request

from . import libversion


_LOGGER = logging.getLogger(__name__)
_BIZ_TYPE = "kafka_python_sdk_config_and_metrics"
_URLS = [
    "http://track.int.xiaohongshu.com/api/data",  # prod
    "http://track.int.xiaohongshu.com/api/data",  # staging
    "http://track.sit.xiaohongshu.com/api/data",  # sit
    "http://track.sit.xiaohongshu.com/api/data",  # test
    "localhost:8123",  # local
    "",  # unknown
]
_ENV_MAP = {
    "prod": 0,
    "staging": 1,
    "sit": 2,
    "test": 3,
    "local": 4,
    "": 5,
}


class EnvUtil:
    _config_env = ""
    _xhs_env = os.getenv("XHS_ENV", "")
    _job_env = os.getenv("JOB_ENV", "")
    _appid = os.getenv("APPID", "")
    _region = os.getenv("XHS_REGION", "")
    _service = os.getenv("XHS_SERVICE", "")
    _zone = os.getenv("XHS_ZONE", "")
    _host_ip = None
    _host_name = None

    @classmethod
    def set_config_env(cls, config_env):
        cls._config_env = config_env or ""

    @classmethod
    def get_env(cls):
        if cls._config_env:
            return cls._config_env
        if cls._job_env:
            return cls._job_env
        return cls._xhs_env

    @classmethod
    def get_env_type(cls):
        return _ENV_MAP.get(cls.get_env(), _ENV_MAP[""])

    @classmethod
    def get_xhs_env(cls):
        return cls._xhs_env

    @classmethod
    def get_job_env(cls):
        return cls._job_env

    @classmethod
    def get_config_env(cls):
        return cls._config_env

    @classmethod
    def get_appid(cls):
        return cls._appid

    @classmethod
    def get_region(cls):
        return cls._region

    @classmethod
    def get_service(cls):
        return cls._service

    @classmethod
    def get_zone(cls):
        return cls._zone

    @classmethod
    def get_host_ip(cls):
        if cls._host_ip is None:
            cls._host_ip = cls._resolve_host_ip()
        return cls._host_ip

    @classmethod
    def get_host_name(cls):
        if cls._host_name is None:
            cls._host_name = cls._resolve_host_name()
        return cls._host_name

    @classmethod
    def get_env_from_sources(cls, env_vars):
        for env_var in env_vars:
            val = os.getenv(env_var, "")
            if val:
                return val
        return ""

    @classmethod
    def _resolve_host_ip(cls):
        env_ip = os.getenv("POD_IP", "")
        if env_ip:
            return env_ip

        ip = cls._get_interface_ip("eth0")
        if ip:
            return ip
        ip = cls._get_interface_ip("en0")
        if ip:
            return ip
        return "127.0.0.1"

    @classmethod
    def _resolve_host_name(cls):
        host_name = os.getenv("HOSTNAME", "")
        if host_name and host_name != "localhost":
            return host_name
        try:
            return socket.gethostname()
        except Exception:
            return "localhost"

    @classmethod
    def _get_interface_ip(cls, name):
        if os.name != "posix":
            return ""
        try:
            import fcntl
            import struct

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            ifreq = struct.pack("256s", name[:15].encode("utf-8"))
            res = fcntl.ioctl(sock.fileno(), 0x8915, ifreq)
            return socket.inet_ntoa(res[20:24])
        except Exception:
            return ""


class MetricsHelper:
    @staticmethod
    def add_metrics_to_json(metrics, dest):
        for key, val in metrics.items():
            dest[key] = str(val)

    @staticmethod
    def parse_stats_for_metrics(stats, dest):
        for key, val in stats.items():
            if isinstance(val, (dict, list)):
                continue
            dest[key] = val

        topics_value = stats.get("topics")
        topics_list = []
        topic_names = []
        if isinstance(topics_value, list):
            topics_list = topics_value
        elif isinstance(topics_value, dict):
            for name, topic in topics_value.items():
                if isinstance(topic, dict):
                    topic_copy = dict(topic)
                    topic_copy["topic_name"] = name
                    topics_list.append(topic_copy)

        for topic in topics_list:
            topic_name = topic.get("topic_name")
            if topic_name:
                topic_names.append(topic_name)
            topic.pop("partitions", None)

        dest["topic_names"] = ",".join(topic_names)
        dest["topics"] = topics_list

        cgrp = stats.get("cgrp")
        if isinstance(cgrp, dict):
            for key, val in cgrp.items():
                dest[key] = val


class MetricsCollector:
    def __init__(self, client, conf, client_type):
        self._client = client
        self._conf = conf or {}
        self._client_type = client_type
        self._base_metrics = self._get_base_metrics()
        self._config_metrics = self._get_config_metrics()

    def get_metrics_json(self):
        metrics = {}
        metrics["metrics_id"] = self._generate_guid()
        MetricsHelper.add_metrics_to_json(self._base_metrics, metrics)

        stats = self._get_stats()
        if stats:
            MetricsHelper.parse_stats_for_metrics(stats, metrics)

        MetricsHelper.add_metrics_to_json(self._config_metrics, metrics)
        return json.dumps(metrics, separators=(",", ":"))

    def _get_base_metrics(self):
        client_version = libversion()[0]
        return {
            "client_type": self._client_type,
            "app_id": EnvUtil.get_appid(),
            "xhs_service": EnvUtil.get_service(),
            "xhs_region": EnvUtil.get_region(),
            "xhs_zone": EnvUtil.get_zone(),
            "xhs_env": EnvUtil.get_xhs_env(),
            "config_env": EnvUtil.get_config_env(),
            "job_env": EnvUtil.get_job_env(),
            "host_name": EnvUtil.get_host_name(),
            "host_ip": EnvUtil.get_host_ip(),
            "client_version": client_version,
        }

    def _get_config_metrics(self):
        metrics = {}
        for key, val in self._conf.items():
            if val is None:
                continue
            val_str = str(val)
            if "0x" in val_str:
                continue
            metrics[key] = val_str
        return metrics

    def _get_stats(self):
        try:
            stats_json = self._client.stats_collect()
        except Exception:
            _LOGGER.exception("stats_collect failed")
            return {}
        if not stats_json:
            return {}
        try:
            return json.loads(stats_json)
        except Exception:
            _LOGGER.exception("stats JSON parse failed")
            return {}

    def _generate_guid(self):
        hi = random.getrandbits(64)
        lo = random.getrandbits(64)
        return (
            f"{(hi >> 32) & 0xffffffff:08x}"
            f"{(hi >> 16) & 0xffff:04x}"
            f"{hi & 0xffff:04x}"
            f"{(lo >> 48) & 0xffff:04x}"
            f"{lo & 0xffffffffffff:012x}"
        )


class MetricsSender:
    def __init__(self, client, conf, client_type, interval=300):
        self._conf = conf or {}
        self._collector = MetricsCollector(client, self._conf, client_type)
        self._interval = max(int(interval), 1)
        self._url = self._resolve_url()
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        if not self._url:
            return False
        if self._thread:
            return True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="MetricsSender", daemon=True
        )
        self._thread.start()
        return True

    def stop(self):
        if not self._thread:
            return
        self._stop_event.set()
        self._thread.join()
        self._thread = None

    def _run(self):
        while not self._stop_event.is_set():
            self._sleep_interval()
            if self._stop_event.is_set():
                break
            try:
                self._do_task()
            except Exception:
                _LOGGER.exception("metrics send failed")

    def _sleep_interval(self):
        for _ in range(self._interval):
            if self._stop_event.is_set():
                return
            time.sleep(1)

    def _do_task(self):
        metrics_json = self._collector.get_metrics_json()
        self._send_metrics(metrics_json)

    def _send_metrics(self, metrics_json):
        payload = metrics_json.encode("utf-8")
        headers = {"Biz-Type": _BIZ_TYPE}
        if len(payload) > 512:
            payload = gzip.compress(payload)
            headers["Content-Encoding"] = "gzip"

        url = self._normalize_url(self._url)
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()

    def _resolve_url(self):
        url = self._conf.get("metrics.collect.url", "")
        if url:
            return url

        config_env = self._conf.get("env.config", "")
        if config_env:
            EnvUtil.set_config_env(config_env)

        env_type = EnvUtil.get_env_type()
        if 0 <= env_type < len(_URLS):
            return _URLS[env_type]
        return ""

    def _normalize_url(self, url):
        if not url:
            return url
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"http://{url}"
