"""
常驻守护管理器
全程保活层：监控所有组件健康状态，异常自动修复重启，杜绝停机。
保证执行+优化双流程持续跑通，是系统的最后一道防线。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional

from shared.shared_data_models import (
    ComponentHealth,
    EventType,
    HealthStatus,
    SystemEvent,
)
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from shared.unified_logging_service import AgentLoggingService


class GuardianWatchItem:
    """守护监控项"""

    def __init__(
        self,
        name: str,
        health_check: Callable[[], ComponentHealth],
        restart_action: Optional[Callable[[], bool]] = None,
        critical: bool = True,
    ):
        self.name = name
        self.health_check = health_check
        self.restart_action = restart_action
        self.critical = critical
        self.consecutive_failures: int = 0
        self.last_health: Optional[ComponentHealth] = None
        self.last_restart_time: float = 0.0


class ResidentGuardianManager:
    """
    常驻守护管理器
    - 定期检查所有组件健康状态
    - 异常组件自动重启
    - 维护系统整体可用性
    - 提供系统健康仪表盘数据
    """

    def __init__(
        self,
        config: Dict[str, Any],
        logging_service: AgentLoggingService,
        recovery_handler: ExceptionRecoveryHandler,
    ):
        self._config = config
        self._log = logging_service.get_logger("guardian_manager")
        self._logging_svc = logging_service
        self._recovery = recovery_handler

        # 监控项注册表
        self._watch_items: Dict[str, GuardianWatchItem] = {}
        self._lock = threading.RLock()

        # 守护线程
        self._guardian_thread: Optional[threading.Thread] = None
        self._is_guarding: bool = False
        self._stop_event = threading.Event()

        # 配置
        self._check_interval = config.get("health_check_interval_seconds", 10.0)
        self._max_errors = config.get("max_component_errors_before_restart", 5)
        self._auto_restart = config.get("auto_restart_enabled", True)

        # 统计
        self._total_health_checks: int = 0
        self._total_restarts: int = 0
        self._guardian_start_time: float = 0.0

    # ── 组件注册 ──

    def register_component(
        self,
        name: str,
        health_check: Callable[[], ComponentHealth],
        restart_action: Optional[Callable[[], bool]] = None,
        critical: bool = True,
    ) -> None:
        """注册需要守护的组件"""
        with self._lock:
            self._watch_items[name] = GuardianWatchItem(
                name=name,
                health_check=health_check,
                restart_action=restart_action,
                critical=critical,
            )
        self._log.info(f"注册守护组件: {name} (critical={critical})")

    def unregister_component(self, name: str) -> None:
        """取消守护组件"""
        with self._lock:
            self._watch_items.pop(name, None)
        self._log.info(f"取消守护组件: {name}")

    # ── 守护控制 ──

    def start_guarding(self) -> None:
        """启动守护线程"""
        with self._lock:
            if self._is_guarding:
                self._log.warning("守护线程已在运行")
                return
            self._is_guarding = True
        self._stop_event.clear()
        self._guardian_start_time = time.time()
        self._guardian_thread = threading.Thread(
            target=self._guardian_loop,
            name="guardian_thread",
            daemon=True,
        )
        self._guardian_thread.start()
        self._log.info("常驻守护线程已启动")

    def stop_guarding(self) -> None:
        """停止守护线程"""
        self._is_guarding = False
        self._stop_event.set()
        if self._guardian_thread and self._guardian_thread.is_alive():
            self._guardian_thread.join(timeout=5.0)
        self._log.info("常驻守护线程已停止")

    # ── 健康检查 ──

    def check_all_components(self) -> Dict[str, ComponentHealth]:
        """手动触发一次全组件健康检查"""
        results = {}
        with self._lock:
            items = list(self._watch_items.values())

        for item in items:
            try:
                health = item.health_check()
                health.last_check = time.time()
                with self._lock:
                    item.last_health = health
                    # 状态变化处理
                    if health.status == HealthStatus.HEALTHY:
                        item.consecutive_failures = 0
                    elif health.status in (HealthStatus.DEGRADED, HealthStatus.UNHEALTHY):
                        item.consecutive_failures += 1
                results[item.name] = health

                if health.status == HealthStatus.HEALTHY:
                    self._recovery.reset_component_errors(item.name)

                elif health.status in (HealthStatus.DEGRADED, HealthStatus.UNHEALTHY):
                    # 发送健康检查事件
                    self._logging_svc.emit_event(SystemEvent(
                        event_type=EventType.SYSTEM_HEALTH_CHECK,
                        source="guardian_manager",
                        data={
                            "component": item.name,
                            "status": health.status.value,
                            "consecutive_failures": item.consecutive_failures,
                        },
                    ))

                    # 自动重启判定
                    with self._lock:
                        failures = item.consecutive_failures
                        has_restart = item.restart_action is not None
                    if (self._auto_restart
                            and has_restart
                            and failures >= self._max_errors):
                        self._restart_component(item)

            except Exception as e:
                self._recovery.handle_exception("guardian_manager", e,
                                                 context={"component": item.name})
                # 创建降级健康报告
                results[item.name] = ComponentHealth(
                    component_name=item.name,
                    status=HealthStatus.UNHEALTHY,
                    last_error=str(e)[:200],
                )

        with self._lock:
            self._total_health_checks += 1
        return results

    def get_system_health(self) -> Dict[str, Any]:
        """获取系统整体健康状态（只读查询，不触发重启）"""
        # 直接读取组件当前状态，不调用 check_all_components 避免副作用
        health_results = {}
        with self._lock:
            for name, item in self._watch_items.items():
                if item.last_health:
                    health_results[name] = item.last_health
                else:
                    health_results[name] = ComponentHealth(component_name=name)

        healthy = sum(1 for h in health_results.values() if h.status == HealthStatus.HEALTHY)
        total = len(health_results)
        degraded = sum(1 for h in health_results.values() if h.status == HealthStatus.DEGRADED)
        unhealthy = sum(1 for h in health_results.values() if h.status == HealthStatus.UNHEALTHY)

        overall = HealthStatus.HEALTHY
        if unhealthy > 0:
            overall = HealthStatus.UNHEALTHY
        elif degraded > 0:
            overall = HealthStatus.DEGRADED

        return {
            "overall_status": overall.value,
            "total_components": total,
            "healthy": healthy,
            "degraded": degraded,
            "unhealthy": unhealthy,
            "components": {
                name: {
                    "status": h.status.value,
                    "error_count": h.error_count,
                    "last_error": h.last_error,
                    "recovery_count": h.recovery_count,
                }
                for name, h in health_results.items()
            },
            "guardian_stats": {
                "uptime_seconds": time.time() - self._guardian_start_time if self._guardian_start_time else 0,
                "total_health_checks": self._total_health_checks,
                "total_restarts": self._total_restarts,
                "is_guarding": self._is_guarding,
            },
        }

    # ── 内部方法 ──

    def _guardian_loop(self) -> None:
        """守护主循环"""
        # 首次等待一个完整间隔，给系统初始化留出时间
        self._stop_event.wait(timeout=self._check_interval)
        while not self._stop_event.is_set():
            try:
                self.check_all_components()
            except Exception as e:
                self._recovery.handle_exception("guardian_manager", e)

            # 等待下一次检查（可被中断）
            self._stop_event.wait(timeout=self._check_interval)

    def _restart_component(self, item: GuardianWatchItem) -> bool:
        """尝试重启组件"""
        self._log.warning(
            f"组件 {item.name} 连续失败 {item.consecutive_failures} 次，"
            f"触发自动重启"
        )

        if not item.restart_action:
            self._log.error(f"组件 {item.name} 未注册重启动作，无法自动恢复")
            return False

        try:
            success = item.restart_action()
            if success:
                with self._lock:
                    item.consecutive_failures = 0
                    item.last_restart_time = time.time()
                    self._total_restarts += 1
                self._log.info(f"组件 {item.name} 重启成功")
                self._logging_svc.emit_event(SystemEvent(
                    event_type=EventType.COMPONENT_RECOVERED,
                    source="guardian_manager",
                    data={"component": item.name, "action": "restart"},
                ))
                return True
            else:
                self._log.error(f"组件 {item.name} 重启失败")
                return False

        except Exception as e:
            self._recovery.handle_exception("guardian_manager", e,
                                             context={"component": item.name, "action": "restart"})
            return False

    def start(self) -> None:
        """启动守护管理器（调用 start_guarding）"""
        self.start_guarding()

    def stop(self) -> None:
        """停止守护管理器（调用 stop_guarding）"""
        self.stop_guarding()
