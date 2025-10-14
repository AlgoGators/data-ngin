from prometheus_client import CollectorRegistry, start_http_server

_registry = None

def get_registry():
    global _registry
    if _registry is None:
        _registry = CollectorRegistry()
    return _registry

def start_metrics_server(port: int = 8003):
    # Expose /metrics for THIS process at http://localhost:<port>/metrics
    start_http_server(port, registry=get_registry())
    return port
