"""
结构化 JSON 日志模块
===================

本模块实现了生产级的结构化日志系统，输出 JSON 格式的日志。
支持日志分级、上下文管理、字段过滤、敏感信息脱敏等。

主要功能:
- JSON 格式输出
- 结构化字段支持
- 上下文绑定和传播
- 敏感信息脱敏
- 日志轮转和归档
- 多输出目标支持

使用示例:
    from observability.structured_logging import StructuredLogger, get_logger
    
    logger = get_logger("my_module")
    logger.info("Operation completed", extra={"user_id": 123, "duration_ms": 45})
"""

import sys
import os
import json
import time
import logging
import threading
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Callable, Union
from dataclasses import dataclass, field
from functools import wraps
from contextvars import ContextVar
import socket
import uuid

# 上下文变量用于跨异步边界传播日志上下文
_log_context: ContextVar[Dict[str, Any]] = ContextVar('log_context', default={})


@dataclass
class LoggingConfig:
    """
    日志配置类
    
    定义结构化日志系统的所有配置选项。
    
    Attributes:
        service_name: 服务名称
        environment: 运行环境
        level: 日志级别
        output_format: 输出格式（json, text）
        include_timestamp: 是否包含时间戳
        include_hostname: 是否包含主机名
        include_pid: 是否包含进程ID
        include_caller: 是否包含调用者信息
        timestamp_format: 时间戳格式
        sensitive_fields: 敏感字段列表（需要脱敏）
        mask_char: 脱敏字符
        extra_fields: 额外的静态字段
        output_streams: 输出流列表
        pretty_print: 是否美化输出（用于调试）
    """
    service_name: str = "evolution-agent"
    environment: str = "development"
    level: str = "INFO"
    output_format: str = "json"
    include_timestamp: bool = True
    include_hostname: bool = True
    include_pid: bool = True
    include_caller: bool = False
    timestamp_format: str = "iso"  # iso, epoch, epoch_ms
    sensitive_fields: List[str] = field(default_factory=lambda: [
        "password", "passwd", "pwd", "secret", "token", "api_key", "apikey",
        "authorization", "auth", "credential", "private_key", "access_token"
    ])
    mask_char: str = "*"
    extra_fields: Dict[str, Any] = field(default_factory=dict)
    output_streams: List[Any] = field(default_factory=lambda: [sys.stdout])
    pretty_print: bool = False
    
    def validate(self) -> bool:
        """验证配置"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.level.upper() not in valid_levels:
            raise ValueError(f"无效的日志级别: {self.level}")
        return True


class SensitiveDataFilter:
    """
    敏感数据过滤器
    
    自动检测并脱敏日志中的敏感信息。
    """
    
    def __init__(self, sensitive_fields: List[str], mask_char: str = "*"):
        """
        初始化过滤器
        
        Args:
            sensitive_fields: 敏感字段名称列表
            mask_char: 脱敏字符
        """
        self._sensitive_fields = set(field.lower() for field in sensitive_fields)
        self._mask_char = mask_char
    
    def is_sensitive(self, key: str) -> bool:
        """检查字段是否敏感"""
        return key.lower() in self._sensitive_fields
    
    def mask(self, value: Any) -> str:
        """脱敏值"""
        if value is None:
            return "null"
        str_value = str(value)
        if len(str_value) <= 2:
            return self._mask_char * len(str_value)
        # 保留首尾字符
        return str_value[0] + self._mask_char * (len(str_value) - 2) + str_value[-1]
    
    def filter_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """过滤字典中的敏感数据"""
        result = {}
        for key, value in data.items():
            if self.is_sensitive(key):
                result[key] = self.mask(value)
            elif isinstance(value, dict):
                result[key] = self.filter_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    self.filter_dict(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        return result


class JSONFormatter(logging.Formatter):
    """
    JSON 格式化器
    
    将日志记录格式化为 JSON 字符串。
    """
    
    def __init__(
        self,
        config: LoggingConfig,
        sensitive_filter: Optional[SensitiveDataFilter] = None
    ):
        """
        初始化格式化器
        
        Args:
            config: 日志配置
            sensitive_filter: 敏感数据过滤器
        """
        super().__init__()
        self._config = config
        self._sensitive_filter = sensitive_filter or SensitiveDataFilter(
            config.sensitive_fields,
            config.mask_char
        )
        
        # 预计算静态字段
        self._static_fields = {
            "service": config.service_name,
            "env": config.environment,
        }
        
        if config.include_hostname:
            self._static_fields["hostname"] = socket.gethostname()
        
        if config.include_pid:
            self._static_fields["pid"] = os.getpid()
        
        # 添加额外字段
        self._static_fields.update(config.extra_fields)
    
    def format(self, record: logging.LogRecord) -> str:
        """格式化日志记录为 JSON"""
        # 基础日志数据
        log_data = {
            "timestamp": self._get_timestamp(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # 添加静态字段
        log_data.update(self._static_fields)
        
        # 添加调用者信息
        if self._config.include_caller:
            log_data["caller"] = {
                "file": record.filename,
                "line": record.lineno,
                "function": record.funcName,
                "module": record.module,
            }
        
        # 添加上下文
        context = _log_context.get()
        if context:
            log_data["context"] = context
        
        # 添加额外字段（从 record 中提取）
        if hasattr(record, "structured_data"):
            structured_data = record.structured_data
            if isinstance(structured_data, dict):
                log_data["data"] = structured_data
        
        # 添加异常信息
        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            log_data["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value),
                "stacktrace": self._format_stacktrace(exc_tb),
            }
        
        # 过滤敏感数据
        log_data = self._sensitive_filter.filter_dict(log_data)

        # 生成 JSON
        try:
            if self._config.pretty_print:
                return json.dumps(log_data, ensure_ascii=False, indent=2, default=str)
            else:
                return json.dumps(log_data, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            # JSON 序列化失败时回退到纯文本
            return f"{record.levelname}: {record.getMessage()} [json_serialization_failed]"
    
    def _get_timestamp(self) -> Union[str, float]:
        """获取时间戳"""
        now = datetime.now(timezone.utc)
        
        if self._config.timestamp_format == "iso":
            return now.isoformat()
        elif self._config.timestamp_format == "epoch":
            return now.timestamp()
        elif self._config.timestamp_format == "epoch_ms":
            return int(now.timestamp() * 1000)
        else:
            return now.isoformat()
    
    def _format_stacktrace(self, tb) -> List[str]:
        """格式化堆栈跟踪"""
        if tb is None:
            return []
        return traceback.format_tb(tb)


class StructuredLogRecord:
    """
    结构化日志记录
    
    用于构建带有丰富上下文的日志记录。
    """
    
    def __init__(self, logger: "StructuredLogger", level: int, message: str):
        self._logger = logger
        self._level = level
        self._message = message
        self._data: Dict[str, Any] = {}
        self._exc_info: Optional[Any] = None
    
    def data(self, **kwargs) -> "StructuredLogRecord":
        """添加数据字段"""
        self._data.update(kwargs)
        return self
    
    def exception(self, exc: Optional[Exception] = None) -> "StructuredLogRecord":
        """附加异常信息"""
        self._exc_info = sys.exc_info() if exc is None else (type(exc), exc, exc.__traceback__)
        return self
    
    def log(self) -> None:
        """发送日志"""
        self._logger._log(
            self._level,
            self._message,
            args=(),
            exc_info=self._exc_info,
            extra={"structured_data": self._data}
        )


class StructuredLogger:
    """
    结构化日志器
    
    提供丰富的日志记录功能，支持结构化数据、上下文管理等。
    
    使用示例:
        logger = StructuredLogger("my_module")
        
        # 基本日志
        logger.info("Operation started")
        
        # 带数据的日志
        logger.info("User logged in", user_id=123, ip="192.168.1.1")
        
        # 链式调用
        logger.with_level("INFO").data(user_id=123).message("User action").log()
        
        # 上下文管理
        with logger.context(request_id="abc123"):
            logger.info("Processing request")
    """
    
    def __init__(
        self,
        name: str,
        config: Optional[LoggingConfig] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        初始化结构化日志器
        
        Args:
            name: 日志器名称
            config: 日志配置
            context: 初始上下文
        """
        self._name = name
        self._config = config or LoggingConfig()
        self._logger = logging.getLogger(name)
        self._context = context or {}
        
        # 确保日志器有处理器
        if not self._logger.handlers:
            self._setup_handler()
    
    def _setup_handler(self):
        """设置日志处理器"""
        handler = logging.StreamHandler(sys.stdout)
        formatter = JSONFormatter(self._config)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(self._config.level.upper())
    
    def _log(self, level: int, message: str, args: tuple = (), 
             exc_info: Any = None, extra: Optional[Dict[str, Any]] = None):
        """内部日志方法"""
        # 合并上下文
        merged_extra = extra or {}
        if self._context:
            existing_data = merged_extra.get("structured_data", {})
            if isinstance(existing_data, dict):
                existing_data.update(self._context)
                merged_extra["structured_data"] = existing_data
        
        self._logger.log(level, message, *args, exc_info=exc_info, extra=merged_extra)
    
    def debug(self, message: str, **kwargs):
        """记录 DEBUG 级别日志"""
        self._log(logging.DEBUG, message, extra={"structured_data": kwargs})
    
    def info(self, message: str, **kwargs):
        """记录 INFO 级别日志"""
        self._log(logging.INFO, message, extra={"structured_data": kwargs})
    
    def warning(self, message: str, **kwargs):
        """记录 WARNING 级别日志"""
        self._log(logging.WARNING, message, extra={"structured_data": kwargs})
    
    def error(self, message: str, exc: Optional[Exception] = None, **kwargs):
        """记录 ERROR 级别日志"""
        exc_info = sys.exc_info() if exc is None else (type(exc), exc, exc.__traceback__)
        self._log(logging.ERROR, message, exc_info=exc_info, extra={"structured_data": kwargs})
    
    def critical(self, message: str, exc: Optional[Exception] = None, **kwargs):
        """记录 CRITICAL 级别日志"""
        exc_info = sys.exc_info() if exc is None else (type(exc), exc, exc.__traceback__)
        self._log(logging.CRITICAL, message, exc_info=exc_info, extra={"structured_data": kwargs})
    
    def exception(self, message: str, **kwargs):
        """记录异常日志（等同于 error 但自动捕获异常）"""
        self._log(logging.ERROR, message, exc_info=True, extra={"structured_data": kwargs})
    
    def with_level(self, level: Union[int, str]) -> StructuredLogRecord:
        """创建指定级别的日志记录"""
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)
        return StructuredLogRecord(self, level, "")
    
    def bind(self, **kwargs) -> "StructuredLogger":
        """绑定上下文字段，返回新的日志器"""
        new_context = {**self._context, **kwargs}
        return StructuredLogger(self._name, self._config, new_context)
    
    def context(self, **kwargs):
        """
        上下文管理器，临时绑定字段
        
        使用示例:
            with logger.context(request_id="abc"):
                logger.info("Processing")
        """
        return LogContextManager(kwargs)


class LogContextManager:
    """日志上下文管理器"""
    
    def __init__(self, context: Dict[str, Any]):
        self._context = context
        self._token = None
    
    def __enter__(self):
        current = _log_context.get()
        new_context = {**current, **self._context}
        self._token = _log_context.set(new_context)
        return self
    
    def __exit__(self, *args):
        if self._token is not None:
            _log_context.reset(self._token)


class LoggingManager:
    """
    日志管理器
    
    管理应用程序的所有日志器，提供统一的配置和初始化。
    
    使用示例:
        manager = LoggingManager(config)
        manager.setup()
        
        logger = manager.get_logger("my_module")
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
    
    def __init__(self, config: Optional[LoggingConfig] = None):
        """初始化日志管理器"""
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._initialized = False
        self._config = config or LoggingConfig()
        self._config.validate()
        self._loggers: Dict[str, StructuredLogger] = {}
        
        self._initialized = True
    
    def setup(self) -> None:
        """设置全局日志配置"""
        # 配置根日志器
        root_logger = logging.getLogger()
        root_logger.setLevel(self._config.level.upper())
        
        # 清除现有处理器
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # 添加 JSON 处理器
        for stream in self._config.output_streams:
            handler = logging.StreamHandler(stream)
            formatter = JSONFormatter(self._config)
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
    
    def get_logger(self, name: str) -> StructuredLogger:
        """获取或创建日志器"""
        return self._loggers.setdefault(name, StructuredLogger(name, self._config))
    
    def set_level(self, level: str) -> None:
        """设置全局日志级别"""
        self._config.level = level.upper()
        logging.getLogger().setLevel(self._config.level)
        for logger in self._loggers.values():
            logger._logger.setLevel(self._config.level)


# 装饰器
def logged(
    logger_name: Optional[str] = None,
    level: str = "DEBUG",
    include_args: bool = True,
    include_result: bool = False
) -> Callable:
    """
    日志装饰器
    
    自动记录函数调用。
    
    使用示例:
        @logged(level="INFO")
        def my_function(arg1, arg2):
            ...
    """
    def decorator(func: Callable) -> Callable:
        name = logger_name or func.__module__
        logger = get_logger(name)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            
            # 记录调用
            call_data = {"function": func_name}
            if include_args:
                call_data["args"] = args
                call_data["kwargs"] = kwargs
            
            logger.debug(f"Calling {func_name}", **call_data)
            
            try:
                result = func(*args, **kwargs)
                
                # 记录结果
                result_data = {"function": func_name}
                if include_result:
                    result_data["result"] = result
                
                logger.debug(f"Completed {func_name}", **result_data)
                return result
                
            except Exception as e:
                logger.error(
                    f"Failed {func_name}",
                    exc=e,
                    function=func_name,
                    error_type=type(e).__name__
                )
                raise
        
        return wrapper
    return decorator


# 便捷函数
def get_logger(name: str) -> StructuredLogger:
    """获取结构化日志器"""
    return LoggingManager().get_logger(name)


def setup_logging(config: Optional[LoggingConfig] = None, **kwargs) -> LoggingManager:
    """设置日志系统"""
    if config is None:
        config = LoggingConfig(**kwargs)
    manager = LoggingManager(config)
    manager.setup()
    return manager


def bind_context(**kwargs) -> LogContextManager:
    """绑定日志上下文"""
    return LogContextManager(kwargs)


# 预定义的日志字段
class LogFields:
    """常用日志字段常量"""
    
    # 请求相关
    REQUEST_ID = "request_id"
    USER_ID = "user_id"
    SESSION_ID = "session_id"
    
    # 任务相关
    TASK_ID = "task_id"
    TASK_TYPE = "task_type"
    TASK_STATUS = "task_status"
    
    # 性能相关
    DURATION_MS = "duration_ms"
    LATENCY_MS = "latency_ms"
    
    # 错误相关
    ERROR_TYPE = "error_type"
    ERROR_CODE = "error_code"
    STACK_TRACE = "stack_trace"
    
    # 系统相关
    HOSTNAME = "hostname"
    PID = "pid"
    THREAD_ID = "thread_id"


# 导出公共接口
__all__ = [
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
]
