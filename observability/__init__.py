"""
可观测性模块
============

本模块提供生产级可观测性体系，包括：
- 分布式追踪 (tracing)
- Prometheus 指标 (metrics)
- 结构化日志 (structured_logging)
- 实时仪表盘 (dashboard)

快速开始:
    from observability import TracingManager, MetricsManager, get_logger, DashboardManager
    
    # 初始化追踪
    tracing = TracingManager(service_name="my-service")
    
    # 初始化指标
    metrics = MetricsManager(namespace="my_app")
    
    # 获取日志器
    logger = get_logger("my_module")
    
    # 获取仪表盘
    dashboard = DashboardManager()
"""

from observability.tracing import (
    TracingConfig,
    TracingManager,
    SpanContextManager,
    NoOpSpan,
    traced,
    traced_async,
    get_tracer,
    get_current_span,
    create_span,
    init_tracing,
    shutdown_tracing,
    OTEL_AVAILABLE,
    OTLP_AVAILABLE,
    JAEGER_AVAILABLE,
)

from observability.metrics import (
    MetricsConfig,
    MetricsManager,
    CounterWrapper,
    GaugeWrapper,
    HistogramWrapper,
    SummaryWrapper,
    EvolutionAgentMetrics,
    timed,
    counted,
    inprogress,
    get_metrics_manager,
    get_counter,
    get_gauge,
    get_histogram,
    sanitize_metric_name,
    sanitize_label_name,
    PROMETHEUS_AVAILABLE,
)

from observability.structured_logging import (
    LoggingConfig,
    LoggingManager,
    StructuredLogger,
    StructuredLogRecord,
    JSONFormatter,
    SensitiveDataFilter,
    LogContextManager,
    LogFields,
    logged,
    get_logger,
    setup_logging,
    bind_context,
)

from observability.dashboard import (
    DashboardConfig,
    DashboardManager,
    DashboardAPI,
    MetricsCollector,
    HealthChecker,
    AlertManager,
    HealthStatus,
    Alert,
    MetricDataPoint,
    get_dashboard,
    get_system_overview,
    get_health_status,
)


__all__ = [
    # Tracing
    'TracingConfig',
    'TracingManager',
    'SpanContextManager',
    'NoOpSpan',
    'traced',
    'traced_async',
    'get_tracer',
    'get_current_span',
    'create_span',
    'init_tracing',
    'shutdown_tracing',
    'OTEL_AVAILABLE',
    'OTLP_AVAILABLE',
    'JAEGER_AVAILABLE',
    
    # Metrics
    'MetricsConfig',
    'MetricsManager',
    'CounterWrapper',
    'GaugeWrapper',
    'HistogramWrapper',
    'SummaryWrapper',
    'EvolutionAgentMetrics',
    'timed',
    'counted',
    'inprogress',
    'get_metrics_manager',
    'get_counter',
    'get_gauge',
    'get_histogram',
    'sanitize_metric_name',
    'sanitize_label_name',
    'PROMETHEUS_AVAILABLE',
    
    # Logging
    'LoggingConfig',
    'LoggingManager',
    'StructuredLogger',
    'StructuredLogRecord',
    'JSONFormatter',
    'SensitiveDataFilter',
    'LogContextManager',
    'LogFields',
    'logged',
    'get_logger',
    'setup_logging',
    'bind_context',
    
    # Dashboard
    'DashboardConfig',
    'DashboardManager',
    'DashboardAPI',
    'MetricsCollector',
    'HealthChecker',
    'AlertManager',
    'HealthStatus',
    'Alert',
    'MetricDataPoint',
    'get_dashboard',
    'get_system_overview',
    'get_health_status',
]

__version__ = "1.0.0"
