"""
统一日志与监控服务
提供结构化日志、性能追踪、事件广播等能力，所有模块共用。
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from shared.shared_data_models import EventType, SystemEvent


class ColoredFormatter(logging.Formatter):
    """终端彩色日志格式化器"""

    COLORS = {
        "DEBUG": "\033[36m",     # 青色
        "INFO": "\033[32m",      # 绿色
        "WARNING": "\033[33m",   # 黄色
        "ERROR": "\033[31m",     # 红色
        "CRITICAL": "\033[35m",  # 紫色
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        # 保存原始 levelname，格式化后恢复，避免影响其他处理器
        original_levelname = record.levelname
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        result = super().format(record)
        record.levelname = original_levelname
        return result


class EventCallback:
    """事件回调包装器"""

    def __init__(self, callback: Callable, filter_types: Optional[List[EventType]] = None):
        self.callback = callback
        self.filter_types = set(filter_types) if filter_types else None

    def should_handle(self, event: SystemEvent) -> bool:
        if self.filter_types is None:
            return True
        return event.event_type in self.filter_types


class AgentLoggingService:
    """Agent统一日志与事件服务"""

    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._loggers: Dict[str, logging.Logger] = {}
        self._event_callbacks: List[EventCallback] = []
        self._event_history: List[SystemEvent] = []
        self._max_history = 1000
        self._event_lock = threading.Lock()
        self._setup_root_logger()

    # ── 日志接口 ──

    def get_logger(self, name: str) -> logging.Logger:
        """获取命名日志器"""
        if name not in self._loggers:
            logger = logging.getLogger(f"evolution.{name}")
            logger.setLevel(self._config.get("log_level", "INFO"))
            logger.propagate = True
            self._loggers[name] = logger
        return self._loggers[name]

    def debug(self, module: str, message: str, **kwargs: Any) -> None:
        self.get_logger(module).debug(message, **kwargs)

    def info(self, module: str, message: str, **kwargs: Any) -> None:
        self.get_logger(module).info(message, **kwargs)

    def warning(self, module: str, message: str, **kwargs: Any) -> None:
        self.get_logger(module).warning(message, **kwargs)

    def error(self, module: str, message: str, **kwargs: Any) -> None:
        self.get_logger(module).error(message, **kwargs)

    def critical(self, module: str, message: str, **kwargs: Any) -> None:
        self.get_logger(module).critical(message, **kwargs)

    # ── 事件总线接口 ──

    def emit_event(self, event: SystemEvent) -> None:
        """发布系统事件"""
        with self._event_lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history:]

        # 迭代前复制列表，防止回调中修改列表引发 RuntimeError
        for callback in list(self._event_callbacks):
            if callback.should_handle(event):
                try:
                    callback.callback(event)
                except Exception as e:
                    self.error("event_bus", f"事件回调执行失败: {e}")

    def subscribe(self, callback: Callable, filter_types: Optional[List[EventType]] = None) -> None:
        """订阅系统事件"""
        with self._event_lock:
            self._event_callbacks.append(EventCallback(callback, filter_types))

    def unsubscribe(self, callback: Callable) -> None:
        """取消订阅"""
        with self._event_lock:
            self._event_callbacks = [
                cb for cb in self._event_callbacks if cb.callback != callback
            ]

    def get_recent_events(self, count: int = 50) -> List[SystemEvent]:
        """获取最近的事件记录"""
        with self._event_lock:
            count = max(1, min(count, len(self._event_history)))
            return self._event_history[-count:]

    def get_events_by_type(self, event_type: EventType) -> List[SystemEvent]:
        """按类型筛选事件"""
        with self._event_lock:
            return [e for e in self._event_history if e.event_type == event_type]

    # ── 性能追踪 ──

    class PerformanceTracker:
        """简易性能追踪上下文管理器"""

        def __init__(self, logging_service: "AgentLoggingService", operation: str):
            self._svc = logging_service
            self._operation = operation
            self._start: float = 0.0

        def __enter__(self) -> "AgentLoggingService.PerformanceTracker":
            self._start = time.time()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            elapsed = time.time() - self._start
            if exc_type:
                self._svc.warning("perf_tracker",
                                  f"[{self._operation}] 异常退出, 耗时 {elapsed:.3f}s")
            else:
                self._svc.debug("perf_tracker",
                                f"[{self._operation}] 完成, 耗时 {elapsed:.3f}s")

    def track_performance(self, operation: str) -> PerformanceTracker:
        """创建性能追踪器"""
        return self.PerformanceTracker(self, operation)

    # ── 内部方法 ──

    def _setup_root_logger(self) -> None:
        """配置根日志器"""
        root = logging.getLogger("evolution")
        root.setLevel(logging.DEBUG)

        # 清除已有处理器
        root.handlers.clear()

        fmt = self._config.get(
            "format_string",
            "%(asctime)s | %(name)-28s | %(levelname)-8s | %(message)s"
        )
        formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

        # 控制台处理器
        if self._config.get("console_enabled", True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(ColoredFormatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
            root.addHandler(console_handler)

        # 文件处理器
        if self._config.get("file_enabled", True):
            file_path = self._config.get("file_path", "./workspace/logs/agent.log")
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                file_path,
                maxBytes=self._config.get("max_file_size_mb", 50) * 1024 * 1024,
                backupCount=self._config.get("backup_count", 5),
                encoding="utf-8",
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
