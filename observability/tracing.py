"""
OpenTelemetry 分布式追踪模块
============================

本模块实现了生产级的分布式追踪系统，基于 OpenTelemetry 标准。
支持跨服务的请求追踪、性能分析和问题诊断。

主要功能:
- 自动和手动 Span 创建
- 上下文传播（跨进程/跨服务）
- 追踪数据导出（Jaeger、OTLP）
- 装饰器简化追踪使用
- 错误追踪和异常记录

使用示例:
    from observability.tracing import TracingManager, traced
    
    # 初始化追踪管理器
    tracing = TracingManager(service_name="evolution-agent")
    
    # 使用装饰器追踪函数
    @traced("task_execution")
    def execute_task(task_id):
        ...
"""

import functools
import time
import threading
from typing import Optional, Dict, Any, Callable, TypeVar, ParamSpec
from dataclasses import dataclass, field
from contextlib import contextmanager
import logging

# OpenTelemetry 导入
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME
    from opentelemetry.trace import Status, StatusCode, SpanKind
    from opentelemetry.context import Context
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    
    # 导出器
    try:
        from opentelemetry.exporter.otlp.trace_exporter import OTLPSpanExporter
        OTLP_AVAILABLE = True
    except ImportError:
        OTLP_AVAILABLE = False
    
    try:
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
        JAEGER_AVAILABLE = True
    except ImportError:
        JAEGER_AVAILABLE = False
    
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    OTLP_AVAILABLE = False
    JAEGER_AVAILABLE = False

# 类型定义
P = ParamSpec('P')
T = TypeVar('T')

# 本地日志
logger = logging.getLogger(__name__)


@dataclass
class TracingConfig:
    """
    追踪配置类
    
    定义追踪系统的所有配置选项，包括服务名称、采样率、导出器配置等。
    
    Attributes:
        service_name: 服务名称，用于标识追踪来源
        service_version: 服务版本
        environment: 运行环境（development, staging, production）
        sampling_rate: 采样率，0.0-1.0 之间
        exporter_type: 导出器类型（otlp, jaeger, console）
        exporter_endpoint: 导出器端点地址
        enable_console_export: 是否启用控制台导出（用于调试）
        max_export_batch_size: 批量导出最大数量
        export_timeout_ms: 导出超时时间（毫秒）
    """
    service_name: str = "evolution-agent"
    service_version: str = "1.0.0"
    environment: str = "development"
    sampling_rate: float = 1.0
    exporter_type: str = "console"  # otlp, jaeger, console
    exporter_endpoint: Optional[str] = None
    enable_console_export: bool = True
    max_export_batch_size: int = 512
    export_timeout_ms: int = 30000
    
    def validate(self) -> bool:
        """验证配置有效性"""
        if not 0.0 <= self.sampling_rate <= 1.0:
            raise ValueError(f"采样率必须在 0.0-1.0 之间，当前: {self.sampling_rate}")
        if self.exporter_type not in ["otlp", "jaeger", "console"]:
            raise ValueError(f"不支持的导出器类型: {self.exporter_type}")
        return True


class SpanContextManager:
    """
    Span 上下文管理器
    
    提供便捷的 Span 生命周期管理，支持 with 语句。
    自动处理 Span 的开始、结束和异常记录。
    
    使用示例:
        with SpanContextManager(tracer, "operation_name") as span:
            span.set_attribute("key", "value")
            # 执行操作
    """
    
    def __init__(
        self,
        tracer: Any,
        name: str,
        kind: Any = None,
        attributes: Optional[Dict[str, Any]] = None,
        parent: Optional[Any] = None
    ):
        """
        初始化 Span 上下文管理器
        
        Args:
            tracer: OpenTelemetry Tracer 实例
            name: Span 名称
            kind: Span 类型（CLIENT, SERVER, PRODUCER, CONSUMER, INTERNAL）
            attributes: 初始属性字典
            parent: 父 Span 或上下文
        """
        self._tracer = tracer
        self._name = name
        self._kind = kind
        self._attributes = attributes or {}
        self._parent = parent
        self._span = None
        self._span_cm = None
        self._span_token = None
        self._start_time = None
    
    def __enter__(self):
        """进入上下文，创建并开始 Span"""
        if not OTEL_AVAILABLE:
            return self
        
        kind = self._kind or SpanKind.INTERNAL
        
        # 创建 Span
        self._span = self._tracer.start_span(
            self._name,
            kind=kind,
            attributes=self._attributes
        )
        
        # 设置为当前 Span
        self._span_cm = trace.use_span(self._span, end_on_exit=True)
        self._span_token = self._span_cm.__enter__()
        self._start_time = time.time()
        
        return self._span
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文，结束 Span 并记录异常"""
        if self._span_cm is not None:
            self._span_cm.__exit__(exc_type, exc_val, exc_tb)
        return False
    
    @property
    def span(self):
        """获取当前 Span"""
        return self._span


class TracingManager:
    """
    追踪管理器
    
    核心追踪管理类，负责初始化 OpenTelemetry、创建 Tracer、
    配置导出器和管理追踪上下文。
    
    生产级特性:
    - 多种导出器支持（OTLP、Jaeger、Console）
    - 采样率控制
    - 资源属性自动注入
    - 线程安全的 Span 管理
    - 优雅关闭支持
    
    使用示例:
        # 初始化
        tracing = TracingManager(
            service_name="evolution-agent",
            exporter_type="otlp",
            exporter_endpoint="http://localhost:4317"
        )
        
        # 创建 Span
        with tracing.span("operation") as span:
            span.set_attribute("key", "value")
        
        # 关闭
        tracing.shutdown()
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """单例模式确保全局唯一的追踪管理器"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config: Optional[TracingConfig] = None, **kwargs):
        """
        初始化追踪管理器
        
        Args:
            config: 追踪配置对象
            **kwargs: 配置参数（用于快速配置，会覆盖 config 中的值）
        """
        with TracingManager._lock:
            # 避免重复初始化
            if hasattr(self, '_initialized') and self._initialized:
                return

            self._initialized = False
            self._config = config or TracingConfig()
            
            # 应用 kwargs 配置
            for key, value in kwargs.items():
                if hasattr(self._config, key):
                    setattr(self._config, key, value)
            
            self._config.validate()
            
            # OpenTelemetry 组件
            self._tracer_provider = None
            self._tracer = None
            self._propagator = None
            
            # 初始化 OpenTelemetry
            self._setup_opentelemetry()
            
            self._initialized = True
            logger.info(f"追踪管理器初始化完成: service={self._config.service_name}")
    
    def _setup_opentelemetry(self):
        """设置 OpenTelemetry 组件"""
        if not OTEL_AVAILABLE:
            logger.warning("OpenTelemetry 不可用，追踪功能将被禁用")
            return
        
        # 创建资源
        resource = Resource.create({
            SERVICE_NAME: self._config.service_name,
            "service.version": self._config.service_version,
            "service.environment": self._config.environment,
            "telemetry.sdk.language": "python",
        })
        
        # 创建 TracerProvider
        self._tracer_provider = TracerProvider(resource=resource)
        
        # 配置导出器
        self._configure_exporters()
        
        # 设置全局 TracerProvider
        trace.set_tracer_provider(self._tracer_provider)
        
        # 创建 Tracer
        self._tracer = trace.get_tracer(
            self._config.service_name,
            self._config.service_version
        )
        
        # 创建传播器
        self._propagator = TraceContextTextMapPropagator()
    
    def _configure_exporters(self):
        """配置 Span 导出器"""
        exporters = []
        
        # 根据配置类型添加导出器
        if self._config.exporter_type == "otlp" and OTLP_AVAILABLE:
            endpoint = self._config.exporter_endpoint or "http://localhost:4317"
            otlp_exporter = OTLPSpanExporter(endpoint=endpoint)
            exporters.append(("OTLP", otlp_exporter))
            logger.info(f"配置 OTLP 导出器: {endpoint}")
        
        elif self._config.exporter_type == "jaeger" and JAEGER_AVAILABLE:
            jaeger_exporter = JaegerExporter(
                agent_host_name="localhost",
                agent_port=6831,
            )
            exporters.append(("Jaeger", jaeger_exporter))
            logger.info("配置 Jaeger 导出器")
        
        # 控制台导出器（调试用）
        if self._config.enable_console_export:
            from opentelemetry.sdk.trace.export import ConsoleSpanExporter
            console_exporter = ConsoleSpanExporter()
            exporters.append(("Console", console_exporter))
        
        # 添加 Span 处理器
        for name, exporter in exporters:
            if name == "Console":
                # 控制台使用简单处理器，立即输出
                processor = SimpleSpanProcessor(exporter)
            else:
                # 其他使用批量处理器
                processor = BatchSpanProcessor(
                    exporter,
                    max_export_batch_size=self._config.max_export_batch_size,
                    export_timeout_millis=self._config.export_timeout_ms,
                )
            self._tracer_provider.add_span_processor(processor)
    
    def get_tracer(self) -> Any:
        """获取 Tracer 实例"""
        return self._tracer
    
    def span(
        self,
        name: str,
        kind: Any = None,
        attributes: Optional[Dict[str, Any]] = None
    ) -> SpanContextManager:
        """
        创建 Span 上下文管理器
        
        Args:
            name: Span 名称
            kind: Span 类型
            attributes: 初始属性
        
        Returns:
            SpanContextManager 实例
        
        使用示例:
            with tracing.span("database_query") as span:
                span.set_attribute("db.system", "postgresql")
        """
        return SpanContextManager(
            self._tracer,
            name,
            kind=kind,
            attributes=attributes
        )
    
    def start_span(
        self,
        name: str,
        kind: Any = None,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        手动启动 Span
        
        Args:
            name: Span 名称
            kind: Span 类型
            attributes: 初始属性
        
        Returns:
            Span 实例
        """
        if not OTEL_AVAILABLE or self._tracer is None:
            return NoOpSpan()
        
        kind = kind or SpanKind.INTERNAL
        return self._tracer.start_span(name, kind=kind, attributes=attributes)
    
    def get_current_span(self) -> Any:
        """获取当前活动的 Span"""
        if not OTEL_AVAILABLE:
            return NoOpSpan()
        return trace.get_current_span()
    
    def inject_context(self, carrier: Dict[str, str]) -> None:
        """
        将当前追踪上下文注入到载体中
        
        用于跨进程/跨服务传播追踪上下文。
        
        Args:
            carrier: 载体字典（如 HTTP headers）
        
        使用示例:
            headers = {}
            tracing.inject_context(headers)
            # headers 现在包含 traceparent 和 tracestate
        """
        if self._propagator is None:
            return
        self._propagator.inject(carrier)
    
    def extract_context(self, carrier: Dict[str, str]) -> Any:
        """
        从载体中提取追踪上下文
        
        Args:
            carrier: 载体字典（如 HTTP headers）
        
        Returns:
            提取的 Context 对象
        """
        if self._propagator is None:
            return None
        return self._propagator.extract(carrier)
    
    def shutdown(self) -> None:
        """
        关闭追踪管理器
        
        刷新所有待导出的 Span 并关闭导出器。
        应在应用程序退出前调用。
        """
        if self._tracer_provider is not None:
            self._tracer_provider.shutdown()
            logger.info("追踪管理器已关闭")
        self._initialized = False
        self._tracer_provider = None
        self._tracer = None
    
    def force_flush(self, timeout_ms: int = 30000) -> bool:
        """
        强制刷新所有待导出的 Span
        
        Args:
            timeout_ms: 超时时间（毫秒）
        
        Returns:
            是否成功刷新
        """
        if self._tracer_provider is not None:
            return self._tracer_provider.force_flush(timeout_millis=timeout_ms)
        return True


class NoOpSpan:
    """
    空操作 Span
    
    当 OpenTelemetry 不可用时使用的占位 Span。
    所有操作都是空操作，确保代码在没有追踪时也能正常运行。
    """
    
    def set_attribute(self, key: str, value: Any) -> "NoOpSpan":
        return self
    
    def set_attributes(self, attributes: Dict[str, Any]) -> "NoOpSpan":
        return self
    
    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> "NoOpSpan":
        return self
    
    def record_exception(self, exception: Exception, attributes: Optional[Dict[str, Any]] = None) -> None:
        pass
    
    def set_status(self, status: Any) -> None:
        pass
    
    def update_name(self, name: str) -> None:
        pass
    
    def end(self, end_time: Optional[int] = None) -> None:
        pass
    
    def is_recording(self) -> bool:
        return False


def traced(
    name: Optional[str] = None,
    kind: Any = None,
    attributes: Optional[Dict[str, Any]] = None
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    追踪装饰器
    
    自动为函数创建 Span，记录执行时间和异常。
    
    Args:
        name: Span 名称（默认使用函数名）
        kind: Span 类型
        attributes: 静态属性
    
    Returns:
        装饰后的函数
    
    使用示例:
        @traced("database_operation")
        def query_database(sql: str):
            ...
        
        @traced(attributes={"db.system": "postgresql"})
        def execute_query(query):
            ...
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        span_name = name or func.__name__
        
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # 获取全局追踪管理器
            tracing = TracingManager()
            
            if not OTEL_AVAILABLE or tracing._tracer is None:
                return func(*args, **kwargs)
            
            # 创建 Span
            with tracing.span(span_name, kind=kind, attributes=attributes) as span:
                # 记录函数参数
                if args:
                    span.set_attribute("args_count", len(args))
                if kwargs:
                    span.set_attribute("kwargs_keys", list(kwargs.keys()))
                
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    # 异常已在 SpanContextManager 中记录
                    raise
        
        return wrapper
    return decorator


def traced_async(
    name: Optional[str] = None,
    kind: Any = None,
    attributes: Optional[Dict[str, Any]] = None
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    异步追踪装饰器
    
    为异步函数创建 Span。
    
    使用示例:
        @traced_async("async_operation")
        async def fetch_data(url: str):
            ...
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        span_name = name or func.__name__
        
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            tracing = TracingManager()
            
            if not OTEL_AVAILABLE or tracing._tracer is None:
                return await func(*args, **kwargs)
            
            with tracing.span(span_name, kind=kind, attributes=attributes) as span:
                if args:
                    span.set_attribute("args_count", len(args))
                if kwargs:
                    span.set_attribute("kwargs_keys", list(kwargs.keys()))
                
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    raise
        
        return wrapper
    return decorator


# 便捷函数
def get_tracer() -> Any:
    """获取全局 Tracer"""
    return TracingManager().get_tracer()


def get_current_span() -> Any:
    """获取当前 Span"""
    return TracingManager().get_current_span()


def create_span(name: str, **kwargs) -> SpanContextManager:
    """创建 Span 上下文管理器"""
    return TracingManager().span(name, **kwargs)


# 模块级别的追踪管理器（延迟初始化）
_global_tracing: Optional[TracingManager] = None


def init_tracing(config: Optional[TracingConfig] = None, **kwargs) -> TracingManager:
    """
    初始化全局追踪管理器
    
    Args:
        config: 追踪配置
        **kwargs: 配置参数
    
    Returns:
        TracingManager 实例
    """
    global _global_tracing
    _global_tracing = TracingManager(config, **kwargs)
    return _global_tracing


def shutdown_tracing() -> None:
    """关闭全局追踪管理器"""
    global _global_tracing
    if _global_tracing is not None:
        _global_tracing.shutdown()
        _global_tracing = None


# 导出公共接口
__all__ = [
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
]
