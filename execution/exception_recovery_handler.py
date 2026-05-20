"""
异常恢复与容错处理器
捕获系统运行中的各类异常，执行局部恢复、自动重试、降级处理，
确保整体执行+优化大循环永不中断。
"""

from __future__ import annotations

import functools
import threading
import time
import traceback
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

from shared.shared_data_models import (
    ComponentHealth,
    EventType,
    HealthStatus,
    SystemEvent,
)
from shared.unified_logging_service import AgentLoggingService


F = TypeVar("F", bound=Callable)


class RecoveryStrategy(str, Enum):
    """恢复策略"""
    RETRY = "retry"               # 重试
    SKIP = "skip"                 # 跳过
    FALLBACK = "fallback"         # 降级处理
    RESTART_COMPONENT = "restart_component"  # 重启组件
    ESCALATE = "escalate"         # 上报（不自动处理）


@dataclass
class ErrorRecord:
    """错误记录"""
    error_id: str
    component: str
    error_type: str
    error_message: str
    timestamp: float
    recovery_strategy: RecoveryStrategy
    recovery_success: bool = False
    retry_count: int = 0
    traceback_str: str = ""


@dataclass
class RecoveryAction:
    """恢复动作定义"""
    name: str
    strategy: RecoveryStrategy
    max_retries: int = 3
    retry_delay: float = 1.0
    fallback_handler: Optional[Callable] = None
    timeout: float = 30.0


class ExceptionRecoveryHandler:
    """异常恢复处理器：保障系统持续运行"""

    def __init__(self, config: Dict[str, Any], logging_service: AgentLoggingService):
        self._config = config
        self._log = logging_service.get_logger("recovery_handler")
        self._logging_svc = logging_service

        # 错误记录
        self._error_records: List[ErrorRecord] = []
        self._component_error_counts: Dict[str, int] = {}
        self._lock = threading.RLock()

        # 恢复动作注册表
        self._recovery_actions: Dict[str, RecoveryAction] = {}
        self._component_health: Dict[str, ComponentHealth] = {}

        # 配置
        self._max_errors_before_restart = config.get(
            "max_component_errors_before_restart", 5
        )
        self._recovery_timeout = config.get("recovery_timeout_seconds", 30.0)

        # 注册默认恢复动作
        self._register_default_actions()

    # ── 公共接口 ──

    def register_recovery(self, component: str, action: RecoveryAction) -> None:
        """为组件注册恢复动作"""
        self._recovery_actions[component] = action
        self._log.debug(f"注册恢复动作: component={component}, strategy={action.strategy.value}")

    def handle_exception(self, component: str, error: Exception,
                         context: Optional[Dict[str, Any]] = None) -> bool:
        """
        处理异常：尝试恢复并返回是否成功。
        仅暂停局部流程，不终止全局循环。
        """
        error_id = f"{component}_{int(time.time() * 1000)}"
        tb_str = traceback.format_exc()

        self._log.error(f"[{component}] 异常捕获: {type(error).__name__}: {error}")

        with self._lock:
            # 记录错误
            self._component_error_counts[component] = \
                self._component_error_counts.get(component, 0) + 1
            error_count = self._component_error_counts[component]

            record = ErrorRecord(
                error_id=error_id,
                component=component,
                error_type=type(error).__name__,
                error_message=str(error),
                timestamp=time.time(),
                recovery_strategy=RecoveryStrategy.RETRY,
                traceback_str=tb_str,
            )
            self._error_records.append(record)

            # 更新组件健康状态
            health = self._component_health.get(component, ComponentHealth(component_name=component))
            health.error_count += 1
            health.last_error = str(error)[:200]
            if error_count >= self._max_errors_before_restart:
                health.status = HealthStatus.UNHEALTHY
            elif error_count >= self._max_errors_before_restart // 2:
                health.status = HealthStatus.DEGRADED
            self._component_health[component] = health

        # 发送事件
        self._logging_svc.emit_event(SystemEvent(
            event_type=EventType.COMPONENT_ERROR,
            source=component,
            data={
                "error_id": error_id,
                "error_type": type(error).__name__,
                "error_message": str(error)[:200],
                "error_count": error_count,
                "context": context or {},
            },
        ))

        # 执行恢复
        recovery_success = self._attempt_recovery(component, error, record)

        if recovery_success:
            with self._lock:
                record.recovery_success = True
                health = self._component_health.get(component)
                if health:
                    # 仅在错误计数较低时才标记为RECOVERING
                    # 高错误计数时保持UNHEALTHY/DEGRADED状态
                    current_errors = self._component_error_counts.get(component, 0)
                    if current_errors < self._max_errors_before_restart // 2:
                        health.status = HealthStatus.RECOVERING
                    health.recovery_count += 1
            self._log.info(f"[{component}] 异常恢复成功")
            self._logging_svc.emit_event(SystemEvent(
                event_type=EventType.COMPONENT_RECOVERED,
                source=component,
                data={"error_id": error_id},
            ))
        else:
            self._log.warning(f"[{component}] 自动恢复失败，局部暂停，全局循环继续")

        return recovery_success

    def safe_execute(self, component: str, func: F, *args: Any, **kwargs: Any) -> Any:
        """安全执行包装器：自动捕获异常并恢复"""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.handle_exception(component, e)
            # 返回降级结果或None
            action = self._recovery_actions.get(component)
            if action and action.fallback_handler:
                try:
                    return action.fallback_handler(*args, **kwargs)
                except Exception as fb_err:
                    self._log.error(f"[{component}] 降级处理也失败: {fb_err}")
            return None

    def reset_component_errors(self, component: str) -> None:
        """重置组件错误计数（通常在成功执行后调用）"""
        with self._lock:
            self._component_error_counts[component] = 0
            health = self._component_health.get(component)
            if health:
                health.error_count = 0
                health.status = HealthStatus.HEALTHY

    def get_component_health(self, component: str) -> ComponentHealth:
        """获取组件健康状态"""
        with self._lock:
            health = self._component_health.get(component)
            if health:
                health.last_check = time.time()
                return health
            return ComponentHealth(component_name=component, status=HealthStatus.HEALTHY)

    def get_all_health(self) -> Dict[str, ComponentHealth]:
        """获取所有组件健康状态"""
        with self._lock:
            return dict(self._component_health)

    def get_error_stats(self) -> Dict[str, Any]:
        """获取错误统计"""
        with self._lock:
            total = len(self._error_records)
            recovered = sum(1 for r in self._error_records if r.recovery_success)
            return {
                "total_errors": total,
                "recovered_errors": recovered,
                "unrecovered_errors": total - recovered,
                "recovery_rate": recovered / total if total > 0 else 1.0,
                "components_with_errors": dict(self._component_error_counts),
            }

    # ── 内部方法 ──

    def _attempt_recovery(self, component: str, error: Exception,
                          record: ErrorRecord) -> bool:
        """尝试恢复"""
        action = self._recovery_actions.get(component)
        strategy = action.strategy if action else RecoveryStrategy.RETRY
        max_retries = action.max_retries if action else 3
        retry_delay = action.retry_delay if action else 1.0

        record.recovery_strategy = strategy

        if strategy == RecoveryStrategy.SKIP:
            self._log.info(f"[{component}] 跳过恢复策略，继续运行")
            return True

        if strategy == RecoveryStrategy.RETRY:
            # 对于可重试的异常，这里只是记录策略
            # 实际重试由执行引擎处理
            return True

        if strategy == RecoveryStrategy.FALLBACK:
            return action and action.fallback_handler is not None

        if strategy == RecoveryStrategy.RESTART_COMPONENT:
            self._log.warning(f"[{component}] 需要重启组件，由守护层处理")
            return False  # 交给守护层处理

        return True

    def _register_default_actions(self) -> None:
        """注册默认恢复动作"""
        default_components = [
            "execution_engine", "optimization_analyzer",
            "sync_bridge", "data_collector", "rule_repository",
        ]
        for comp in default_components:
            if comp not in self._recovery_actions:
                self._recovery_actions[comp] = RecoveryAction(
                    name=f"{comp}_default_recovery",
                    strategy=RecoveryStrategy.RETRY,
                    max_retries=3,
                    retry_delay=2.0,
                )


def recovery_guard(component: str):
    """装饰器：为函数添加异常恢复保护"""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # 从参数中尝试获取handler实例
            handler = None
            for arg in args:
                if isinstance(arg, ExceptionRecoveryHandler):
                    handler = arg
                    break
            if handler:
                return handler.safe_execute(component, func, *args, **kwargs)
            try:
                return func(*args, **kwargs)
            except Exception:
                return None
        return wrapper  # type: ignore
    return decorator
