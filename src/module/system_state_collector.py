"""Collect the observability snapshot attached to every agent operation.

The PromQL below is intentionally hard-coded.  It mirrors the supplied
"Online Boutique - KPI & Resources" Grafana dashboard without parsing the
dashboard at runtime.
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .utils import load_config


ONLINE_BOUTIQUE_SERVICES = (
    'adservice',
    'cartservice',
    'checkoutservice',
    'currencyservice',
    'emailservice',
    'frontend',
    'paymentservice',
    'productcatalogservice',
    'recommendationservice',
    'shippingservice',
)

_SERVICE_REGEX = '(' + '|'.join(ONLINE_BOUTIQUE_SERVICES) + ')'

# KPI and resource queries from Online Boutique - KPI & Resources.
PROMETHEUS_QUERIES = {
    'server_request_rate': (
        'sum by (service_name) (rate(traces_spanmetrics_calls_total'
        '{span_kind="SPAN_KIND_SERVER",service_name=~"__SERVICES__"}[2m]))'
    ),
    'server_error_ratio': (
        'sum by (service_name) (rate(traces_spanmetrics_calls_total'
        '{span_kind="SPAN_KIND_SERVER",status_code="STATUS_CODE_ERROR",'
        'service_name=~"__SERVICES__"}[2m])) / '
        'sum by (service_name) (rate(traces_spanmetrics_calls_total'
        '{span_kind="SPAN_KIND_SERVER",service_name=~"__SERVICES__"}[2m]))'
    ),
    'server_mean_latency_ms': (
        'sum by (service_name) (rate(traces_spanmetrics_duration_milliseconds_sum'
        '{span_kind="SPAN_KIND_SERVER",service_name=~"__SERVICES__"}[2m])) / '
        'sum by (service_name) (rate(traces_spanmetrics_duration_milliseconds_count'
        '{span_kind="SPAN_KIND_SERVER",service_name=~"__SERVICES__"}[2m]))'
    ),
    'server_p50_latency_ms': (
        'histogram_quantile(0.50, sum by (le,service_name) '
        '(rate(traces_spanmetrics_duration_milliseconds_bucket'
        '{span_kind="SPAN_KIND_SERVER",service_name=~"__SERVICES__"}[2m])))'
    ),
    'server_p90_latency_ms': (
        'histogram_quantile(0.90, sum by (le,service_name) '
        '(rate(traces_spanmetrics_duration_milliseconds_bucket'
        '{span_kind="SPAN_KIND_SERVER",service_name=~"__SERVICES__"}[2m])))'
    ),
    'server_p95_latency_ms': (
        'histogram_quantile(0.95, sum by (le,service_name) '
        '(rate(traces_spanmetrics_duration_milliseconds_bucket'
        '{span_kind="SPAN_KIND_SERVER",service_name=~"__SERVICES__"}[2m])))'
    ),
    'server_p99_latency_ms': (
        'histogram_quantile(0.99, sum by (le,service_name) '
        '(rate(traces_spanmetrics_duration_milliseconds_bucket'
        '{span_kind="SPAN_KIND_SERVER",service_name=~"__SERVICES__"}[2m])))'
    ),
    'client_request_rate': (
        'sum by (service_name) (rate(traces_spanmetrics_calls_total'
        '{span_kind="SPAN_KIND_CLIENT",service_name=~"__SERVICES__"}[2m]))'
    ),
    'client_error_ratio': (
        'sum by (service_name) (rate(traces_spanmetrics_calls_total'
        '{span_kind="SPAN_KIND_CLIENT",status_code="STATUS_CODE_ERROR",'
        'service_name=~"__SERVICES__"}[2m])) / '
        'sum by (service_name) (rate(traces_spanmetrics_calls_total'
        '{span_kind="SPAN_KIND_CLIENT",service_name=~"__SERVICES__"}[2m]))'
    ),
    'client_mean_latency_ms': (
        'sum by (service_name) (rate(traces_spanmetrics_duration_milliseconds_sum'
        '{span_kind="SPAN_KIND_CLIENT",service_name=~"__SERVICES__"}[2m])) / '
        'sum by (service_name) (rate(traces_spanmetrics_duration_milliseconds_count'
        '{span_kind="SPAN_KIND_CLIENT",service_name=~"__SERVICES__"}[2m]))'
    ),
    'client_p50_latency_ms': (
        'histogram_quantile(0.50, sum by (le,service_name) '
        '(rate(traces_spanmetrics_duration_milliseconds_bucket'
        '{span_kind="SPAN_KIND_CLIENT",service_name=~"__SERVICES__"}[2m])))'
    ),
    'client_p90_latency_ms': (
        'histogram_quantile(0.90, sum by (le,service_name) '
        '(rate(traces_spanmetrics_duration_milliseconds_bucket'
        '{span_kind="SPAN_KIND_CLIENT",service_name=~"__SERVICES__"}[2m])))'
    ),
    'client_p95_latency_ms': (
        'histogram_quantile(0.95, sum by (le,service_name) '
        '(rate(traces_spanmetrics_duration_milliseconds_bucket'
        '{span_kind="SPAN_KIND_CLIENT",service_name=~"__SERVICES__"}[2m])))'
    ),
    'client_p99_latency_ms': (
        'histogram_quantile(0.99, sum by (le,service_name) '
        '(rate(traces_spanmetrics_duration_milliseconds_bucket'
        '{span_kind="SPAN_KIND_CLIENT",service_name=~"__SERVICES__"}[2m])))'
    ),
    'cpu_cores': (
        'sum by (pod) (rate(container_cpu_usage_seconds_total'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*",container!="",container!="POD"}[2m]))'
    ),
    'cpu_limit_percent': (
        '100 * sum by (pod) (rate(container_cpu_usage_seconds_total'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*",container!="",container!="POD"}[2m])) / '
        'sum by (pod) (kube_pod_container_resource_limits'
        '{resource="cpu",namespace="__NAMESPACE__",pod=~"__SERVICES__.*",'
        'container!="",container!="POD"})'
    ),
    'cpu_throttled_periods_per_second': (
        'sum by (pod) (rate(container_cpu_cfs_throttled_periods_total'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*",container!="",container!="POD"}[2m]))'
    ),
    'memory_working_set_mib': (
        'sum by (pod) (container_memory_working_set_bytes'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*",container!="",container!="POD"}) / 1024 / 1024'
    ),
    'memory_limit_percent': (
        '100 * sum by (pod) (container_memory_working_set_bytes'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*",container!="",container!="POD"}) / '
        'sum by (pod) (kube_pod_container_resource_limits'
        '{resource="memory",namespace="__NAMESPACE__",pod=~"__SERVICES__.*",'
        'container!="",container!="POD"})'
    ),
    'memory_rss_mib': (
        'sum by (pod) (container_memory_rss'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*",container!="",container!="POD"}) / 1024 / 1024'
    ),
    'network_receive_bytes_per_second': (
        'sum by (pod) (rate(container_network_receive_bytes_total'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*"}[2m]))'
    ),
    'network_transmit_bytes_per_second': (
        'sum by (pod) (rate(container_network_transmit_bytes_total'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*"}[2m]))'
    ),
    'filesystem_read_bytes_per_second': (
        'sum by (pod) (rate(container_fs_reads_bytes_total'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*",container!="",container!="POD"}[2m]))'
    ),
    'filesystem_write_bytes_per_second': (
        'sum by (pod) (rate(container_fs_writes_bytes_total'
        '{namespace="__NAMESPACE__",pod=~"__SERVICES__.*",container!="",container!="POD"}[2m]))'
    ),
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SystemStateCollector:
    """Fetch Prometheus KPIs/resources and Jaeger dependency data."""

    def __init__(self, namespace: str = 'onlineboutique') -> None:
        config = load_config()
        observability = config.get('observability', {})
        self.namespace = namespace
        self.prometheus_url = os.environ.get(
            'PROMETHEUS_URL',
            observability.get('prometheus_url', 'http://localhost:9090'),
        ).rstrip('/')
        self.jaeger_url = os.environ.get(
            'JAEGER_URL',
            observability.get('jaeger_url', 'http://localhost:16686'),
        ).rstrip('/')
        self.timeout = int(observability.get('request_timeout_seconds', 10))
        self.jaeger_lookback_ms = int(observability.get('jaeger_lookback_ms', 300000))

    def _get_json(self, url: str) -> Any:
        request = Request(url, headers={'Accept': 'application/json'})
        with urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode('utf-8'))

    def _prometheus_query(self, name: str, expression: str) -> tuple[str, dict]:
        expression = expression.replace('__SERVICES__', _SERVICE_REGEX)
        expression = expression.replace('__NAMESPACE__', self.namespace)
        url = f'{self.prometheus_url}/api/v1/query?{urlencode({"query": expression})}'
        try:
            payload = self._get_json(url)
            if payload.get('status') != 'success':
                raise RuntimeError(payload.get('error', 'Prometheus query failed'))
            return name, {
                'query': expression,
                'series': payload.get('data', {}).get('result', []),
            }
        except Exception as exc:  # A failed sensor must not block the agent action.
            return name, {'query': expression, 'error': f'{type(exc).__name__}: {exc}'}

    def query_dependency_trace(self) -> dict:
        end_ms = int(time.time() * 1000)
        query = urlencode({'endTs': end_ms, 'lookback': self.jaeger_lookback_ms})
        url = f'{self.jaeger_url}/api/dependencies?{query}'
        try:
            payload = self._get_json(url)
            dependencies = payload.get('data', payload)
            if isinstance(dependencies, list):
                allowed = set(ONLINE_BOUTIQUE_SERVICES)
                dependencies = [
                    edge for edge in dependencies
                    if edge.get('parent') in allowed or edge.get('child') in allowed
                ]
            return {'url': url, 'dependencies': dependencies}
        except Exception as exc:
            return {'url': url, 'error': f'{type(exc).__name__}: {exc}'}

    def capture(self) -> dict:
        prometheus: dict[str, dict] = {}
        worker_count = min(8, len(PROMETHEUS_QUERIES))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(self._prometheus_query, name, query)
                for name, query in PROMETHEUS_QUERIES.items()
            ]
            for future in as_completed(futures):
                name, result = future.result()
                prometheus[name] = result
        return {
            'captured_at': _utc_now(),
            'namespace': self.namespace,
            'services': list(ONLINE_BOUTIQUE_SERVICES),
            'prometheus': dict(sorted(prometheus.items())),
            'jaeger': {'dependency_trace': self.query_dependency_trace()},
        }

    @staticmethod
    def errors(snapshot: dict) -> list[str]:
        """Return sensor/API errors while allowing valid empty metric series."""
        errors = [
            f'Prometheus {name}: {result["error"]}'
            for name, result in snapshot.get('prometheus', {}).items()
            if 'error' in result
        ]
        dependency_trace = (
            snapshot.get('jaeger', {}).get('dependency_trace', {})
        )
        if 'error' in dependency_trace:
            errors.append(f'Jaeger dependency_trace: {dependency_trace["error"]}')
        return errors
