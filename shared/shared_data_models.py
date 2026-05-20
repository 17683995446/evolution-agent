"""
共享数据模型层
定义Agent系统中所有核心数据结构，包括任务、指标、优化规则、执行记录等。
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


# ──────────────────────── 枚举类型 ────────────────────────

class TaskStatus(str, Enum):
    """任务生命周期状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"


class TaskPriority(str, Enum):
    """任务优先级"""
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class RuleCategory(str, Enum):
    """优化规则分类"""
    PERFORMANCE = "performance"       # 性能优化
    ERROR_REDUCTION = "error_reduction"  # 错误减少
    RESOURCE_SAVING = "resource_saving"  # 资源节约
    FLOW_OPTIMIZATION = "flow_optimization"  # 流程优化
    QUALITY_IMPROVEMENT = "quality_improvement"  # 质量提升


class HealthStatus(str, Enum):
    """组件健康状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    RECOVERING = "recovering"


# ──────────────────────── 任务相关模型 ────────────────────────

@dataclass
class TaskDefinition:
    """任务定义：描述一个待执行任务的完整规格"""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    priority: TaskPriority = TaskPriority.NORMAL
    handler: Optional[str] = None       # 处理函数名称
    handler_params: Dict[str, Any] = field(default_factory=dict)
    max_retries: int = 3
    timeout_seconds: float = 300.0
    tags: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    scheduled_at: Optional[float] = None
    dependencies: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "name": self.name,
            "description": self.description,
            "priority": self.priority.value,
            "handler": self.handler,
            "handler_params": self.handler_params,
            "max_retries": self.max_retries,
            "timeout_seconds": self.timeout_seconds,
            "tags": self.tags,
            "created_at": self.created_at,
            "scheduled_at": self.scheduled_at,
            "dependencies": self.dependencies,
        }


@dataclass
class TaskResult:
    """任务执行结果"""
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    duration_seconds: float = 0.0
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.status == TaskStatus.COMPLETED

    @property
    def duration(self) -> float:
        if self.started_at and self.finished_at:
            return self.finished_at - self.started_at
        return self.duration_seconds


# ──────────────────────── 指标与度量模型 ────────────────────────

@dataclass
class ExecutionMetric:
    """单次执行度量数据"""
    metric_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str = ""
    cycle_number: int = 0
    timestamp: float = field(default_factory=time.time)
    duration_seconds: float = 0.0
    success: bool = False
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    resource_usage: Dict[str, float] = field(default_factory=dict)
    custom_metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CycleSummary:
    """一个完整循环周期的汇总统计"""
    cycle_number: int
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    skipped_tasks: int = 0
    avg_duration: float = 0.0
    total_errors: int = 0
    error_types: Dict[str, int] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        if self.total_tasks == 0:
            return 0.0
        return self.completed_tasks / self.total_tasks

    @property
    def duration(self) -> float:
        end = self.finished_at or time.time()
        return end - self.started_at


# ──────────────────────── 优化规则模型 ────────────────────────

@dataclass
class OptimizationRule:
    """优化规则：由智能优化层生成，用于改进执行流程"""
    rule_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    category: RuleCategory = RuleCategory.PERFORMANCE
    condition: str = ""               # 触发条件描述
    action: str = ""                  # 优化动作描述
    priority: int = 10                # 优先级，数字越小越优先
    created_at: float = field(default_factory=time.time)
    applied_at: Optional[float] = None
    effectiveness_score: float = 0.0  # 效果评分 (0~1)
    apply_count: int = 0
    is_active: bool = True
    source_cycle: int = 0             # 来源周期编号

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "condition": self.condition,
            "action": self.action,
            "priority": self.priority,
            "created_at": self.created_at,
            "applied_at": self.applied_at,
            "effectiveness_score": self.effectiveness_score,
            "apply_count": self.apply_count,
            "is_active": self.is_active,
            "source_cycle": self.source_cycle,
        }


# ──────────────────────── 组件健康模型 ────────────────────────

@dataclass
class ComponentHealth:
    """组件健康状态报告"""
    component_name: str
    status: HealthStatus = HealthStatus.HEALTHY
    last_check: float = field(default_factory=time.time)
    error_count: int = 0
    last_error: Optional[str] = None
    recovery_count: int = 0
    uptime_seconds: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


# ──────────────────────── 事件总线模型 ────────────────────────

class EventType(str, Enum):
    """系统事件类型"""
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_RETRIED = "task_retried"
    CYCLE_STARTED = "cycle_started"
    CYCLE_COMPLETED = "cycle_completed"
    RULE_CREATED = "rule_created"
    RULE_APPLIED = "rule_applied"
    RULE_REVOKED = "rule_revoked"
    COMPONENT_ERROR = "component_error"
    COMPONENT_RECOVERED = "component_recovered"
    SYSTEM_HEALTH_CHECK = "system_health_check"


@dataclass
class SystemEvent:
    """系统事件"""
    event_type: EventType
    source: str = ""
    timestamp: float = field(default_factory=time.time)
    data: Dict[str, Any] = field(default_factory=dict)
    cycle_number: int = 0
