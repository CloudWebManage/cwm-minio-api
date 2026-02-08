from prometheus_client import CollectorRegistry, Counter, Histogram, REGISTRY, multiprocess

from .. import config


MINIO_MC_CALLS_TOTAL = Counter(
    "cwm_minio_api_minio_mc_calls_total",
    "Total MinIO mc CLI calls made by the API.",
    labelnames=("operation", "outcome"),
)
MINIO_MC_CALL_DURATION_SECONDS = Histogram(
    "cwm_minio_api_minio_mc_call_duration_seconds",
    "Duration of MinIO mc CLI calls made by the API.",
    labelnames=("operation", "outcome"),
)
DB_CONN_ACQUIRE_TIME = Histogram(
    "cwm_minio_api_db_connection_acquire_seconds",
    "Time spent acquiring DB connection",
    labelnames=("outcome",),
)
DB_CONNS_TOTAL = Counter(
    "cwm_minio_api_db_connections_total",
    "Total DB connections made",
    labelnames=("outcome",),
)


def get_prometheus_registry():
    if config.PROMETHEUS_MULTIPROC_DIR:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry, config.PROMETHEUS_MULTIPROC_DIR)
        return registry
    else:
        return REGISTRY
