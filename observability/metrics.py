"""
Prometheus 指标导出模块
======================

本模块实现了生产级的 Prometheus 指标收集和导出系统。
支持多种指标类型、自动标签、 Histogram 桶配置等。

主要功能:
- Counter: 单调递增计数器
- Gauge: 可增可减的仪表
- Histogram: 分布统计直方图
- Summary: 分位数统计
- 自动指标注册和导出
- HTTP 端点暴露指标

使用示例:
    from observability.metrics import MetricsManager, counter, histogram
    
    # 初始化指标管理器
    metrics = MetricsManager()
    
    # 创建指标
    request_counter = metrics.counter("requests_total", "Total requests")
    latency_histogram = metrics.histogram("request_latency", "Request latency")
    
    # 使用指标
    request_counter.inc()
    latency_histogram.observe(0.5)
"""

import time
import threading
import re
from typing import Optional, Dict, Any, List, Callable, Union
from dataclasses import dataclass, field
from functools import wraps
import logging

# Prometheus 客户端导入
try:
    from prometheus_client import (
        Counter as PrometheusCounter,
        Gauge as PrometheusGauge,
        Histogram as PrometheusHistogram,
        Summary as PrometheusSummary,
        Info, Enum, GaugeHistogram,
        CollectorRegistry, REGISTRY,
        generate_latest, CONTENT_TYPE_LATEST,
        start_http_server, shutdown_http_server
    )
    from prometheus_client.metrics_core import Metric
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# 本地日志
logger = logging.getLogger(__name__)


def sanitize_metric_name(name: str) -> str:
    """
    清理指标名称，使其符合 Prometheus 命名规范
    
    Prometheus 指标名称必须匹配 [a-zA-Z_:][a-zA-Z0-9_:]* 
    
    Args:
        name: 原始名称
    
    Returns:
        清理后的名称
    """
    # 替换无效字符为下划线
    sanitized = re.sub(r'[^a-zA-Z0-9_:]', '_', name)
    # 确保不以数字开头
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    return sanitized


def sanitize_label_name(name: str) -> str:
    """
    清理标签名称
    
    标签名称必须匹配 [a-zA-Z_][a-zA-Z0-9_]*
    
    Args:
        name: 原始名称
    
    Returns:
        清理后的名称
    """
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    return sanitized


@dataclass
class MetricsConfig:
    """
    指标配置类
    
    定义指标系统的配置选项。
    
    Attributes:
        namespace: 指标命名空间前缀
        subsystem: 子系统名称
        enable_default_metrics: 是否启用默认指标
        http_port: HTTP 服务端口
        http_host: HTTP 服务主机
        default_labels: 默认标签
        histogram_buckets: Histogram 默认桶配置
        prefix_separator: 命名空间分隔符
    """
    namespace: str = "evolution_agent"
    subsystem: str = ""
    enable_default_metrics: bool = True
    http_port: int = 9090
    http_host: str = "0.0.0.0"
    default_labels: Dict[str, str] = field(default_factory=dict)
    histogram_buckets: tuple = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, float('inf'))
    prefix_separator: str = "_"
    
    def validate(self) -> bool:
        """验证配置"""
        if self.http_port < 1 or self.http_port > 65535:
            raise ValueError(f"无效的端口号: {self.http_port}")
        return True


class MetricWrapper:
    """
    指标包装器基类
    
    为 Prometheus 指标提供统一的接口和额外功能。
    """
    
    def __init__(self, metric: Any, name: str, description: str):
        self._metric = metric
        self._name = name
        self._description = description
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def description(self) -> str:
        return self._description


class CounterWrapper(MetricWrapper):
    """
    Counter 指标包装器
    
    Counter 是单调递增的计数器，用于记录累计值。
    如请求总数、错误总数等。
    
    使用示例:
        counter = CounterWrapper(...)
        counter.inc()  # 增加 1
        counter.inc(5)  # 增加 5
        counter.labels(method="GET").inc()  # 带标签
    """
    
    def inc(self, amount: float = 1.0) -> None:
        """增加计数"""
        if PROMETHEUS_AVAILABLE and self._metric:
            self._metric.inc(amount)
    
    def labels(self, **kwargs) -> "CounterWrapper":
        """设置标签并返回新的包装器"""
        if PROMETHEUS_AVAILABLE and self._metric:
            labeled_metric = self._metric.labels(**kwargs)
            return CounterWrapper(labeled_metric, self._name, self._description)
        return self


class GaugeWrapper(MetricWrapper):
    """
    Gauge 指标包装器
    
    Gauge 是可增可减的仪表，用于记录瞬时值。
    如当前温度、内存使用量等。
    
    使用示例:
        gauge = GaugeWrapper(...)
        gauge.set(42)  # 设置值
        gauge.inc()  # 增加 1
        gauge.dec()  # 减少 1
    """
    
    def set(self, value: float) -> None:
        """设置值"""
        if PROMETHEUS_AVAILABLE and self._metric:
            self._metric.set(value)
    
    def inc(self, amount: float = 1.0) -> None:
        """增加值"""
        if PROMETHEUS_AVAILABLE and self._metric:
            self._metric.inc(amount)
    
    def dec(self, amount: float = 1.0) -> None:
        """减少值"""
        if PROMETHEUS_AVAILABLE and self._metric:
            self._metric.dec(amount)
    
    def set_to_current_time(self) -> None:
        """设置为当前时间戳"""
        if PROMETHEUS_AVAILABLE and self._metric:
            self._metric.set_to_current_time()
    
    def track_inprogress(self) -> Callable:
        """追踪进行中的操作"""
        if PROMETHEUS_AVAILABLE and self._metric:
            return self._metric.track_inprogress()
        return lambda: None
    
    def time(self) -> "GaugeTimer":
        """计时上下文管理器"""
        return GaugeTimer(self)
    
    def labels(self, **kwargs) -> "GaugeWrapper":
        """设置标签"""
        if PROMETHEUS_AVAILABLE and self._metric:
            labeled_metric = self._metric.labels(**kwargs)
            return GaugeWrapper(labeled_metric, self._name, self._description)
        return self


class GaugeTimer:
    """Gauge 计时器"""
    
    def __init__(self, gauge: GaugeWrapper):
        self._gauge = gauge
        self._start_time = None
    
    def __enter__(self):
        self._start_time = time.time()
        return self
    
    def __exit__(self, *args):
        if self._start_time:
            duration = time.time() - self._start_time
            self._gauge.set(duration)


class HistogramWrapper(MetricWrapper):
    """
    Histogram 指标包装器
    
    Histogram 用于记录值的分布情况。
    如请求延迟、响应大小等。
    
    使用示例:
        histogram = HistogramWrapper(...)
        histogram.observe(0.5)  # 记录一个值
        with histogram.time():  # 计时
            # 执行操作
            ...
    """
    
    def observe(self, amount: float) -> None:
        """记录一个观察值"""
        if PROMETHEUS_AVAILABLE and self._metric:
            self._metric.observe(amount)
    
    def time(self) -> "HistogramTimer":
        """计时上下文管理器"""
        return HistogramTimer(self)
    
    def labels(self, **kwargs) -> "HistogramWrapper":
        """设置标签"""
        if PROMETHEUS_AVAILABLE and self._metric:
            labeled_metric = self._metric.labels(**kwargs)
            return HistogramWrapper(labeled_metric, self._name, self._description)
        return self


class HistogramTimer:
    """Histogram 计时器"""
    
    def __init__(self, histogram: HistogramWrapper):
        self._histogram = histogram
        self._start_time = None
    
    def __enter__(self):
        self._start_time = time.time()
        return self
    
    def __exit__(self, *args):
        if self._start_time:
            duration = time.time() - self._start_time
            self._histogram.observe(duration)


class SummaryWrapper(MetricWrapper):
    """
    Summary 指标包装器
    
    Summary 用于计算分位数。
    如请求延迟的 P50、P90、P99。
    """
    
    def observe(self, amount: float) -> None:
        """记录一个观察值"""
        if PROMETHEUS_AVAILABLE and self._metric:
            self._metric.observe(amount)
    
    def time(self) -> "SummaryTimer":
        """计时上下文管理器"""
        return SummaryTimer(self)
    
    def labels(self, **kwargs) -> "SummaryWrapper":
        """设置标签"""
        if PROMETHEUS_AVAILABLE and self._metric:
            labeled_metric = self._metric.labels(**kwargs)
            return SummaryWrapper(labeled_metric, self._name, self._description)
        return self


class SummaryTimer:
    """Summary 计时器"""
    
    def __init__(self, summary: SummaryWrapper):
        self._summary = summary
        self._start_time = None
    
    def __enter__(self):
        self._start_time = time.time()
        return self
    
    def __exit__(self, *args):
        if self._start_time:
            duration = time.time() - self._start_time
            self._summary.observe(duration)


class MetricsManager:
    """
    指标管理器
    
    核心指标管理类，负责创建、注册和管理所有指标。
    
    生产级特性:
    - 自动命名空间前缀
    - 默认标签注入
    - 线程安全的指标创建
    - HTTP 端点暴露
    - 默认进程指标
    
    使用示例:
        metrics = MetricsManager(namespace="my_app")
        
        # 创建指标
        counter = metrics.counter("requests_total", "Total requests")
        gauge = metrics.gauge("active_connections", "Active connections")
        histogram = metrics.histogram("latency_seconds", "Request latency")
        
        # 启动 HTTP 服务
        metrics.start_http_server(9090)
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config: Optional[MetricsConfig] = None, **kwargs):
        """初始化指标管理器"""
        with MetricsManager._lock:
            if hasattr(self, '_initialized') and self._initialized:
                return

            self._initialized = False
            self._config = config or MetricsConfig()
            
            # 应用 kwargs
            for key, value in kwargs.items():
                if hasattr(self._config, key):
                    setattr(self._config, key, value)
            
            self._config.validate()
            
            # 指标存储
            self._metrics: Dict[str, MetricWrapper] = {}
            self._metrics_lock = threading.Lock()
            
            # HTTP 服务状态
            self._http_server_started = False
            
            # 注册表
            self._registry = REGISTRY if PROMETHEUS_AVAILABLE else None
            
            # 启用默认指标
            if self._config.enable_default_metrics and PROMETHEUS_AVAILABLE:
                self._setup_default_metrics()
            
            self._initialized = True
            logger.info(f"指标管理器初始化完成: namespace={self._config.namespace}")
    
    def _setup_default_metrics(self):
        """设置默认进程指标"""
        try:
            from prometheus_client import PROCESS_COLLECTOR, PLATFORM_COLLECTOR, GC_COLLECTOR
            # 这些会自动注册
        except ImportError:
            pass
    
    def _build_metric_name(self, name: str) -> str:
        """构建完整的指标名称"""
        parts = [self._config.namespace]
        if self._config.subsystem:
            parts.append(self._config.subsystem)
        parts.append(name)
        return self._config.prefix_separator.join(parts)
    
    def counter(
        self,
        name: str,
        description: str,
        labelnames: Optional[List[str]] = None,
        **kwargs
    ) -> CounterWrapper:
        """
        创建或获取 Counter 指标
        
        Args:
            name: 指标名称
            description: 指标描述
            labelnames: 标签名称列表
        
        Returns:
            CounterWrapper 实例
        """
        full_name = self._build_metric_name(name)
        
        with self._metrics_lock:
            if full_name in self._metrics:
                return self._metrics[full_name]
            
            metric = None
            if PROMETHEUS_AVAILABLE:
                labelnames = [sanitize_label_name(l) for l in (labelnames or [])]
                metric = PrometheusCounter(
                    full_name,
                    description,
                    labelnames=labelnames,
                    registry=self._registry,
                    **kwargs
                )
            
            wrapper = CounterWrapper(metric, full_name, description)
            self._metrics[full_name] = wrapper
            return wrapper
    
    def gauge(
        self,
        name: str,
        description: str,
        labelnames: Optional[List[str]] = None,
        multiprocess_mode: str = "all",
        **kwargs
    ) -> GaugeWrapper:
        """
        创建或获取 Gauge 指标
        
        Args:
            name: 指标名称
            description: 指标描述
            labelnames: 标签名称列表
            multiprocess_mode: 多进程模式
        
        Returns:
            GaugeWrapper 实例
        """
        full_name = self._build_metric_name(name)
        
        with self._metrics_lock:
            if full_name in self._metrics:
                return self._metrics[full_name]
            
            metric = None
            if PROMETHEUS_AVAILABLE:
                labelnames = [sanitize_label_name(l) for l in (labelnames or [])]
                metric = PrometheusGauge(
                    full_name,
                    description,
                    labelnames=labelnames,
                    multiprocess_mode=multiprocess_mode,
                    registry=self._registry,
                    **kwargs
                )
            
            wrapper = GaugeWrapper(metric, full_name, description)
            self._metrics[full_name] = wrapper
            return wrapper
    
    def histogram(
        self,
        name: str,
        description: str,
        labelnames: Optional[List[str]] = None,
        buckets: Optional[tuple] = None,
        **kwargs
    ) -> HistogramWrapper:
        """
        创建或获取 Histogram 指标
        
        Args:
            name: 指标名称
            description: 指标描述
            labelnames: 标签名称列表
            buckets: 桶配置
        
        Returns:
            HistogramWrapper 实例
        """
        full_name = self._build_metric_name(name)
        
        with self._metrics_lock:
            if full_name in self._metrics:
                return self._metrics[full_name]
            
            metric = None
            if PROMETHEUS_AVAILABLE:
                labelnames = [sanitize_label_name(l) for l in (labelnames or [])]
                buckets = buckets or self._config.histogram_buckets
                metric = PrometheusHistogram(
                    full_name,
                    description,
                    labelnames=labelnames,
                    buckets=buckets,
                    registry=self._registry,
                    **kwargs
                )
            
            wrapper = HistogramWrapper(metric, full_name, description)
            self._metrics[full_name] = wrapper
            return wrapper
    
    def summary(
        self,
        name: str,
        description: str,
        labelnames: Optional[List[str]] = None,
        **kwargs
    ) -> SummaryWrapper:
        """
        创建或获取 Summary 指标
        
        Args:
            name: 指标名称
            description: 指标描述
            labelnames: 标签名称列表
        
        Returns:
            SummaryWrapper 实例
        """
        full_name = self._build_metric_name(name)
        
        with self._metrics_lock:
            if full_name in self._metrics:
                return self._metrics[full_name]
            
            metric = None
            if PROMETHEUS_AVAILABLE:
                labelnames = [sanitize_label_name(l) for l in (labelnames or [])]
                metric = PrometheusSummary(
                    full_name,
                    description,
                    labelnames=labelnames,
                    registry=self._registry,
                    **kwargs
                )
            
            wrapper = SummaryWrapper(metric, full_name, description)
            self._metrics[full_name] = wrapper
            return wrapper
    
    def info(self, name: str, description: str, **kwargs) -> Any:
        """创建 Info 指标"""
        full_name = self._build_metric_name(name)
        
        if PROMETHEUS_AVAILABLE:
            return Info(full_name, description, registry=self._registry, **kwargs)
        return None
    
    def get_metric(self, name: str) -> Optional[MetricWrapper]:
        """获取已注册的指标"""
        full_name = self._build_metric_name(name)
        return self._metrics.get(full_name)
    
    def get_all_metrics(self) -> Dict[str, MetricWrapper]:
        """获取所有已注册的指标"""
        return self._metrics.copy()
    
    def generate_latest(self) -> bytes:
        """生成最新的指标数据（Prometheus 文本格式）"""
        if PROMETHEUS_AVAILABLE:
            return generate_latest(self._registry)
        return b""
    
    def start_http_server(self, port: Optional[int] = None, host: Optional[str] = None) -> None:
        """
        启动 HTTP 服务器暴露指标
        
        Args:
            port: 端口号
            host: 主机地址
        """
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Prometheus 不可用，无法启动 HTTP 服务器")
            return
        
        if self._http_server_started:
            logger.warning("HTTP 服务器已启动")
            return
        
        port = port or self._config.http_port
        host = host or self._config.http_host
        
        try:
            start_http_server(port, addr=host, registry=self._registry)
            self._http_server_started = True
            logger.info(f"指标 HTTP 服务器已启动: http://{host}:{port}/metrics")
        except Exception as e:
            logger.error(f"启动 HTTP 服务器失败: {e}")
    
    def shutdown_http_server(self) -> None:
        """关闭 HTTP 服务器"""
        if PROMETHEUS_AVAILABLE and self._http_server_started:
            try:
                shutdown_http_server()
                self._http_server_started = False
                logger.info("指标 HTTP 服务器已关闭")
            except Exception as e:
                logger.error(f"关闭 HTTP 服务器失败: {e}")


# 装饰器
def timed(
    histogram_name: Optional[str] = None,
    description: str = "Operation duration"
) -> Callable:
    """
    计时装饰器
    
    自动记录函数执行时间到 Histogram。
    
    使用示例:
        @timed("operation_duration")
        def my_function():
            ...
    """
    def decorator(func: Callable) -> Callable:
        name = histogram_name or f"{func.__name__}_duration_seconds"
        metrics = MetricsManager()
        histogram = metrics.histogram(name, description)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            with histogram.time():
                return func(*args, **kwargs)
        
        return wrapper
    return decorator


def counted(
    counter_name: Optional[str] = None,
    description: str = "Total count"
) -> Callable:
    """
    计数装饰器
    
    自动记录函数调用次数。
    
    使用示例:
        @counted("function_calls")
        def my_function():
            ...
    """
    def decorator(func: Callable) -> Callable:
        name = counter_name or f"{func.__name__}_calls_total"
        metrics = MetricsManager()
        counter = metrics.counter(name, description)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            counter.inc()
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def inprogress(
    gauge_name: Optional[str] = None,
    description: str = "In-progress operations"
) -> Callable:
    """
    追踪进行中操作的装饰器
    
    使用示例:
        @inprogress("operations_in_progress")
        def my_function():
            ...
    """
    def decorator(func: Callable) -> Callable:
        name = gauge_name or f"{func.__name__}_in_progress"
        metrics = MetricsManager()
        gauge = metrics.gauge(name, description)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            with gauge.track_inprogress():
                return func(*args, **kwargs)
        
        return wrapper
    return decorator


# 便捷函数
def get_metrics_manager() -> MetricsManager:
    """获取全局指标管理器"""
    return MetricsManager()


def get_counter(name: str, description: str = "", **kwargs) -> CounterWrapper:
    """获取或创建 Counter"""
    return MetricsManager().counter(name, description, **kwargs)


def get_gauge(name: str, description: str = "", **kwargs) -> GaugeWrapper:
    """获取或创建 Gauge"""
    return MetricsManager().gauge(name, description, **kwargs)


def get_histogram(name: str, description: str = "", **kwargs) -> HistogramWrapper:
    """获取或创建 Histogram"""
    return MetricsManager().histogram(name, description, **kwargs)


# 预定义的 Evolution Agent 指标
class EvolutionAgentMetrics:
    """
    Evolution Agent 预定义指标
    
    包含常用的指标定义，可直接使用。
    """
    
    def __init__(self, metrics: Optional[MetricsManager] = None):
        self._metrics = metrics or MetricsManager()
        
        # 任务执行指标
        self.tasks_total = self._metrics.counter(
            "tasks_total",
            "Total number of tasks processed",
            labelnames=["status", "type"]
        )
        
        self.task_duration = self._metrics.histogram(
            "task_duration_seconds",
            "Task execution duration in seconds",
            labelnames=["type"]
        )
        
        self.tasks_in_progress = self._metrics.gauge(
            "tasks_in_progress",
            "Number of tasks currently in progress",
            labelnames=["type"]
        )
        
        # 规则指标
        self.rules_total = self._metrics.gauge(
            "rules_total",
            "Total number of rules"
        )
        
        self.rule_evaluations = self._metrics.counter(
            "rule_evaluations_total",
            "Total number of rule evaluations",
            labelnames=["result"]
        )
        
        # 错误指标
        self.errors_total = self._metrics.counter(
            "errors_total",
            "Total number of errors",
            labelnames=["type", "severity"]
        )
        
        # 系统指标
        self.uptime = self._metrics.gauge(
            "uptime_seconds",
            "System uptime in seconds"
        )
        
        self.memory_usage = self._metrics.gauge(
            "memory_usage_bytes",
            "Current memory usage in bytes"
        )


# 导出公共接口
__all__ = [
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
]
