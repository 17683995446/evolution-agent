"""
实时监控仪表盘数据模块
=====================

本模块实现了实时监控仪表盘的数据收集和 API 接口。
支持多种数据源、实时更新、告警规则等。

主要功能:
- 实时指标聚合
- 健康状态检查
- 告警规则评估
- 历史数据查询
- WebSocket 实时推送

使用示例:
    from observability.dashboard import DashboardManager
    
    dashboard = DashboardManager()
    
    # 获取系统概览
    overview = dashboard.get_system_overview()
    
    # 获取实时指标
    metrics = dashboard.get_realtime_metrics()
"""

import time
import threading
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Callable, Union
from dataclasses import dataclass, field, asdict
from collections import deque
from functools import wraps
import logging
import os
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# 本地日志
logger = logging.getLogger(__name__)


@dataclass
class HealthStatus:
    """
    健康状态
    
    表示组件或系统的健康状态。
    
    Attributes:
        name: 组件名称
        status: 状态（healthy, degraded, unhealthy）
        message: 状态消息
        details: 详细信息
        timestamp: 时间戳
        latency_ms: 检查延迟（毫秒）
    """
    name: str
    status: str  # healthy, degraded, unhealthy
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    latency_ms: float = 0.0
    
    def is_healthy(self) -> bool:
        return self.status == "healthy"
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Alert:
    """
    告警
    
    表示一个告警事件。
    
    Attributes:
        id: 告警 ID
        name: 告警名称
        severity: 严重程度（info, warning, critical）
        status: 状态（firing, resolved）
        message: 告警消息
        labels: 标签
        starts_at: 开始时间
        ends_at: 结束时间（已解决时）
        value: 触发值
        threshold: 阈值
    """
    id: str
    name: str
    severity: str  # info, warning, critical
    status: str = "firing"  # firing, resolved
    message: str = ""
    labels: Dict[str, str] = field(default_factory=dict)
    starts_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ends_at: Optional[str] = None
    value: float = 0.0
    threshold: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MetricDataPoint:
    """
    指标数据点
    
    Attributes:
        timestamp: 时间戳
        value: 值
        labels: 标签
    """
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class DashboardConfig:
    """
    仪表盘配置
    
    Attributes:
        refresh_interval_seconds: 刷新间隔（秒）
        history_retention_minutes: 历史数据保留时间（分钟）
        max_data_points: 最大数据点数
        enable_alerts: 是否启用告警
        health_check_interval_seconds: 健康检查间隔（秒）
    """
    refresh_interval_seconds: float = 5.0
    history_retention_minutes: int = 60
    max_data_points: int = 1000
    enable_alerts: bool = True
    health_check_interval_seconds: float = 30.0


class MetricsCollector:
    """
    指标收集器
    
    收集系统和应用指标。
    """
    
    def __init__(self, config: DashboardConfig):
        self._config = config
        self._history: Dict[str, deque] = {}
        self._lock = threading.Lock()
    
    def collect_system_metrics(self) -> Dict[str, Any]:
        """收集系统指标"""
        metrics = {
            "timestamp": time.time(),
            "datetime": datetime.now(timezone.utc).isoformat(),
        }
        
        if not PSUTIL_AVAILABLE:
            metrics["error"] = "psutil not available"
            return metrics
        
        try:
            # CPU 指标
            metrics["cpu"] = {
                "percent": psutil.cpu_percent(interval=0.1),
                "count": psutil.cpu_count(),
                "load_avg": list(os.getloadavg()) if hasattr(os, 'getloadavg') else [0, 0, 0],
            }
            
            # 内存指标
            memory = psutil.virtual_memory()
            metrics["memory"] = {
                "total": memory.total,
                "available": memory.available,
                "used": memory.used,
                "percent": memory.percent,
            }
            
            # 磁盘指标
            disk = psutil.disk_usage('/')
            metrics["disk"] = {
                "total": disk.total,
                "used": disk.used,
                "free": disk.free,
                "percent": disk.percent,
            }
            
            # 网络指标
            net = psutil.net_io_counters()
            metrics["network"] = {
                "bytes_sent": net.bytes_sent,
                "bytes_recv": net.bytes_recv,
                "packets_sent": net.packets_sent,
                "packets_recv": net.packets_recv,
            }
            
            # 进程指标
            process = psutil.Process(os.getpid())
            metrics["process"] = {
                "cpu_percent": process.cpu_percent(),
                "memory_percent": process.memory_percent(),
                "num_threads": process.num_threads(),
                "num_fds": process.num_fds() if hasattr(process, 'num_fds') else 0,
            }
            
        except Exception as e:
            logger.error(f"收集系统指标失败: {e}")
            metrics["error"] = str(e)
        
        return metrics
    
    def record_metric(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """记录指标值"""
        with self._lock:
            if name not in self._history:
                max_points = self._config.max_data_points
                self._history[name] = deque(maxlen=max_points)
            
            point = MetricDataPoint(
                timestamp=time.time(),
                value=value,
                labels=labels or {}
            )
            self._history[name].append(point)
    
    def get_metric_history(
        self,
        name: str,
        duration_minutes: Optional[int] = None
    ) -> List[MetricDataPoint]:
        """获取指标历史数据"""
        duration_minutes = duration_minutes or self._config.history_retention_minutes
        cutoff = time.time() - duration_minutes * 60
        
        with self._lock:
            if name not in self._history:
                return []
            
            return [
                point for point in self._history[name]
                if point.timestamp >= cutoff
            ]
    
    def get_all_metrics(self) -> Dict[str, List[Dict[str, Any]]]:
        """获取所有指标历史"""
        result = {}
        with self._lock:
            for name, history in self._history.items():
                result[name] = [
                    {"timestamp": p.timestamp, "value": p.value, "labels": p.labels}
                    for p in history
                ]
        return result


class HealthChecker:
    """
    健康检查器
    
    执行健康检查并报告状态。
    """
    
    def __init__(self):
        self._checks: Dict[str, Callable[[], HealthStatus]] = {}
        self._lock = threading.Lock()
    
    def register_check(self, name: str, check_func: Callable[[], HealthStatus]) -> None:
        """注册健康检查"""
        with self._lock:
            self._checks[name] = check_func
    
    def unregister_check(self, name: str) -> None:
        """注销健康检查"""
        with self._lock:
            self._checks.pop(name, None)
    
    def run_check(self, name: str) -> Optional[HealthStatus]:
        """执行单个健康检查"""
        with self._lock:
            check_func = self._checks.get(name)
        
        if check_func is None:
            return None
        
        start_time = time.time()
        try:
            status = check_func()
            status.latency_ms = (time.time() - start_time) * 1000
            return status
        except Exception as e:
            return HealthStatus(
                name=name,
                status="unhealthy",
                message=f"Health check failed: {e}",
                latency_ms=(time.time() - start_time) * 1000
            )
    
    def run_all_checks(self) -> Dict[str, HealthStatus]:
        """执行所有健康检查"""
        results = {}
        with self._lock:
            check_names = list(self._checks.keys())
        
        for name in check_names:
            status = self.run_check(name)
            if status:
                results[name] = status
        
        return results
    
    def get_overall_status(self) -> str:
        """获取整体健康状态"""
        results = self.run_all_checks()
        
        if not results:
            return "unknown"
        
        statuses = [r.status for r in results.values()]
        
        if "unhealthy" in statuses:
            return "unhealthy"
        elif "degraded" in statuses:
            return "degraded"
        else:
            return "healthy"


class AlertManager:
    """
    告警管理器
    
    管理告警规则和告警状态。
    """
    
    def __init__(self, config: DashboardConfig):
        self._config = config
        self._rules: Dict[str, Dict[str, Any]] = {}
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_history: deque = deque(maxlen=1000)
        self._lock = threading.Lock()
        self._alert_counter = 0
    
    def add_rule(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        severity: str = "warning",
        message_template: str = "",
        threshold: float = 0.0
    ) -> None:
        """
        添加告警规则
        
        Args:
            name: 规则名称
            condition: 条件函数
            severity: 严重程度
            message_template: 消息模板
            threshold: 阈值
        """
        with self._lock:
            self._rules[name] = {
                "condition": condition,
                "severity": severity,
                "message_template": message_template,
                "threshold": threshold,
            }
    
    def remove_rule(self, name: str) -> None:
        """移除告警规则"""
        with self._lock:
            self._rules.pop(name, None)
    
    def evaluate(self, metrics: Dict[str, Any]) -> List[Alert]:
        """评估所有规则"""
        if not self._config.enable_alerts:
            return []
        
        new_alerts = []
        
        with self._lock:
            rules = dict(self._rules)
        
        for name, rule in rules.items():
            try:
                is_firing = rule["condition"](metrics)
                
                with self._lock:
                    existing = self._active_alerts.get(name)
                    
                    if is_firing and existing is None:
                        # 创建新告警
                        self._alert_counter += 1
                        alert = Alert(
                            id=f"alert-{self._alert_counter}",
                            name=name,
                            severity=rule["severity"],
                            message=rule["message_template"].format(
                                value=metrics.get(name, 0),
                                threshold=rule["threshold"]
                            ),
                            value=metrics.get(name, 0),
                            threshold=rule["threshold"],
                        )
                        self._active_alerts[name] = alert
                        self._alert_history.append(alert)
                        new_alerts.append(alert)
                        
                    elif not is_firing and existing is not None:
                        # 解决告警
                        existing.status = "resolved"
                        existing.ends_at = datetime.now(timezone.utc).isoformat()
                        self._alert_history.append(existing)
                        del self._active_alerts[name]
                        
            except Exception as e:
                logger.error(f"评估告警规则 {name} 失败: {e}")
        
        return new_alerts
    
    def get_active_alerts(self) -> List[Alert]:
        """获取活动告警"""
        with self._lock:
            return list(self._active_alerts.values())
    
    def get_alert_history(self, limit: int = 100) -> List[Alert]:
        """获取告警历史"""
        with self._lock:
            return list(self._alert_history)[-limit:]


class DashboardManager:
    """
    仪表盘管理器
    
    核心仪表盘管理类，整合所有监控功能。
    
    生产级特性:
    - 实时指标收集
    - 健康状态监控
    - 告警规则评估
    - 历史数据存储
    - 线程安全
    
    使用示例:
        dashboard = DashboardManager()
        
        # 注册健康检查
        dashboard.register_health_check("database", check_db)
        
        # 添加告警规则
        dashboard.add_alert_rule("high_cpu", lambda m: m.get("cpu", {}).get("percent", 0) > 80)
        
        # 获取系统概览
        overview = dashboard.get_system_overview()
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
    
    def __init__(self, config: Optional[DashboardConfig] = None):
        """初始化仪表盘管理器"""
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._initialized = False
        self._config = config or DashboardConfig()
        
        # 组件
        self._metrics_collector = MetricsCollector(self._config)
        self._health_checker = HealthChecker()
        self._alert_manager = AlertManager(self._config)
        
        # 状态
        self._start_time = time.time()
        self._last_refresh = 0.0
        self._cached_metrics: Dict[str, Any] = {}
        
        # 注册默认健康检查
        self._setup_default_health_checks()
        
        # 注册默认告警规则
        self._setup_default_alert_rules()
        
        self._initialized = True
        logger.info("仪表盘管理器初始化完成")
    
    def _setup_default_health_checks(self):
        """设置默认健康检查"""
        if not PSUTIL_AVAILABLE:
            return

        def check_memory():
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                return HealthStatus(
                    name="memory",
                    status="unhealthy",
                    message=f"Memory usage too high: {memory.percent:.1f}%",
                    details={"percent": memory.percent}
                )
            elif memory.percent > 80:
                return HealthStatus(
                    name="memory",
                    status="degraded",
                    message=f"Memory usage high: {memory.percent:.1f}%",
                    details={"percent": memory.percent}
                )
            return HealthStatus(
                name="memory",
                status="healthy",
                message=f"Memory usage normal: {memory.percent:.1f}%",
                details={"percent": memory.percent}
            )
        
        def check_disk():
            disk = psutil.disk_usage('/')
            if disk.percent > 90:
                return HealthStatus(
                    name="disk",
                    status="unhealthy",
                    message=f"Disk usage too high: {disk.percent:.1f}%",
                    details={"percent": disk.percent}
                )
            elif disk.percent > 80:
                return HealthStatus(
                    name="disk",
                    status="degraded",
                    message=f"Disk usage high: {disk.percent:.1f}%",
                    details={"percent": disk.percent}
                )
            return HealthStatus(
                name="disk",
                status="healthy",
                message=f"Disk usage normal: {disk.percent:.1f}%",
                details={"percent": disk.percent}
            )
        
        self._health_checker.register_check("memory", check_memory)
        self._health_checker.register_check("disk", check_disk)
    
    def _setup_default_alert_rules(self):
        """设置默认告警规则"""
        self._alert_manager.add_rule(
            name="high_cpu",
            condition=lambda m: m.get("cpu", {}).get("percent", 0) > 80,
            severity="warning",
            message_template="CPU usage is high: {value:.1f}% (threshold: {threshold}%)",
            threshold=80.0
        )
        
        self._alert_manager.add_rule(
            name="high_memory",
            condition=lambda m: m.get("memory", {}).get("percent", 0) > 85,
            severity="warning",
            message_template="Memory usage is high: {value:.1f}% (threshold: {threshold}%)",
            threshold=85.0
        )
        
        self._alert_manager.add_rule(
            name="critical_memory",
            condition=lambda m: m.get("memory", {}).get("percent", 0) > 95,
            severity="critical",
            message_template="Memory usage critical: {value:.1f}% (threshold: {threshold}%)",
            threshold=95.0
        )
    
    def get_system_overview(self) -> Dict[str, Any]:
        """
        获取系统概览
        
        Returns:
            包含系统状态、健康检查、告警等信息的字典
        """
        metrics = self._metrics_collector.collect_system_metrics()
        health = self._health_checker.run_all_checks()
        alerts = self._alert_manager.get_active_alerts()
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": time.time() - self._start_time,
            "overall_health": self._health_checker.get_overall_status(),
            "metrics": metrics,
            "health_checks": {k: v.to_dict() for k, v in health.items()},
            "active_alerts": [a.to_dict() for a in alerts],
            "alert_count": len(alerts),
        }
    
    def get_realtime_metrics(self) -> Dict[str, Any]:
        """获取实时指标"""
        # 检查是否需要刷新
        now = time.time()
        if now - self._last_refresh >= self._config.refresh_interval_seconds:
            self._cached_metrics = self._metrics_collector.collect_system_metrics()
            self._last_refresh = now
            
            # 评估告警
            self._alert_manager.evaluate(self._cached_metrics)
        
        return self._cached_metrics
    
    def get_metrics_history(
        self,
        metric_name: str,
        duration_minutes: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """获取指标历史"""
        history = self._metrics_collector.get_metric_history(metric_name, duration_minutes)
        return [
            {"timestamp": p.timestamp, "value": p.value, "labels": p.labels}
            for p in history
        ]
    
    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        checks = self._health_checker.run_all_checks()
        return {
            "overall": self._health_checker.get_overall_status(),
            "checks": {k: v.to_dict() for k, v in checks.items()},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    
    def get_alerts(self, include_history: bool = False) -> Dict[str, Any]:
        """获取告警信息"""
        active_alerts = self._alert_manager.get_active_alerts()
        result = {
            "active": [a.to_dict() for a in active_alerts],
            "count": len(active_alerts),
        }
        
        if include_history:
            result["history"] = [a.to_dict() for a in self._alert_manager.get_alert_history()]
        
        return result
    
    def register_health_check(self, name: str, check_func: Callable[[], HealthStatus]) -> None:
        """注册健康检查"""
        self._health_checker.register_check(name, check_func)
    
    def unregister_health_check(self, name: str) -> None:
        """注销健康检查"""
        self._health_checker.unregister_check(name)
    
    def add_alert_rule(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        severity: str = "warning",
        message_template: str = "",
        threshold: float = 0.0
    ) -> None:
        """添加告警规则"""
        self._alert_manager.add_rule(name, condition, severity, message_template, threshold)
    
    def remove_alert_rule(self, name: str) -> None:
        """移除告警规则"""
        self._alert_manager.remove_rule(name)
    
    def record_metric(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """记录自定义指标"""
        self._metrics_collector.record_metric(name, value, labels)
    
    def get_all_custom_metrics(self) -> Dict[str, List[Dict[str, Any]]]:
        """获取所有自定义指标"""
        return self._metrics_collector.get_all_metrics()
    
    def refresh(self) -> Dict[str, Any]:
        """强制刷新所有数据"""
        metrics = self._metrics_collector.collect_system_metrics()
        self._cached_metrics = metrics
        self._last_refresh = time.time()
        
        # 评估告警
        new_alerts = self._alert_manager.evaluate(metrics)
        
        return {
            "metrics": metrics,
            "new_alerts": [a.to_dict() for a in new_alerts],
        }


# API 响应构建器
class DashboardAPI:
    """
    仪表盘 API
    
    提供 RESTful API 接口的数据构建。
    """
    
    def __init__(self, dashboard: Optional[DashboardManager] = None):
        self._dashboard = dashboard or DashboardManager()
    
    def overview(self) -> Dict[str, Any]:
        """GET /api/overview"""
        return self._dashboard.get_system_overview()
    
    def metrics(self) -> Dict[str, Any]:
        """GET /api/metrics"""
        return self._dashboard.get_realtime_metrics()
    
    def metrics_history(self, name: str, duration: Optional[int] = None) -> Dict[str, Any]:
        """GET /api/metrics/{name}/history"""
        return {
            "name": name,
            "data": self._dashboard.get_metrics_history(name, duration),
        }
    
    def health(self) -> Dict[str, Any]:
        """GET /api/health"""
        return self._dashboard.get_health_status()
    
    def alerts(self, history: bool = False) -> Dict[str, Any]:
        """GET /api/alerts"""
        return self._dashboard.get_alerts(include_history=history)
    
    def refresh(self) -> Dict[str, Any]:
        """POST /api/refresh"""
        return self._dashboard.refresh()


# 便捷函数
def get_dashboard() -> DashboardManager:
    """获取仪表盘管理器"""
    return DashboardManager()


def get_system_overview() -> Dict[str, Any]:
    """获取系统概览"""
    return DashboardManager().get_system_overview()


def get_health_status() -> Dict[str, Any]:
    """获取健康状态"""
    return DashboardManager().get_health_status()


# 导出公共接口
__all__ = [
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
