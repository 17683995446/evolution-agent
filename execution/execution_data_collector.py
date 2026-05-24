"""
执行数据采集器
实时采集任务执行过程中的各类指标数据，为智能优化层提供分析素材。
支持内存存储与文件持久化，提供聚合统计与趋势分析能力。
"""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from shared.shared_data_models import (
    CycleSummary,
    ExecutionMetric,
    EventType,
    SystemEvent,
    TaskResult,
    TaskStatus,
)
from shared.unified_logging_service import AgentLoggingService


class ExecutionDataCollector:
    """执行数据采集器：收集、存储、分析任务执行指标"""

    def __init__(self, config: Dict[str, Any], logging_service: AgentLoggingService):
        self._config = config
        self._log = logging_service.get_logger("data_collector")
        self._logging_svc = logging_service

        # 存储结构
        self._metrics: List[ExecutionMetric] = []
        self._cycle_summaries: List[CycleSummary] = []
        self._error_registry: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._duration_history: List[float] = []
        self._lock = threading.RLock()

        # 配置
        self._retention_cycles = config.get("metrics_retention_cycles", 100)
        self._persist_path = config.get("metrics_persistence_path", "./workspace/metrics.json")

        # 预计算统计（避免每次遍历）
        self._total_executions = 0
        self._total_success = 0
        self._total_failures = 0
        # 累计统计（永不因 prune 而减少，用于 get_total_stats）
        self._cumulative_executions = 0
        self._cumulative_success = 0

        # 订阅事件
        self._logging_svc.subscribe(self._on_event, filter_types=[
            EventType.TASK_COMPLETED,
            EventType.TASK_FAILED,
            EventType.CYCLE_COMPLETED,
        ])

    # ── 数据采集接口 ──

    def record_task_execution(self, result: TaskResult, cycle_number: int) -> ExecutionMetric:
        """记录一次任务执行数据"""
        metric = ExecutionMetric(
            task_id=result.task_id,
            cycle_number=cycle_number,
            duration_seconds=result.duration,
            success=result.success,
            error_type=result.error[:100] if result.error else None,  # error 现在是 str 类型，直接截取前100字符作为类型标识
            error_message=str(result.error)[:500] if result.error else None,
            resource_usage=result.metadata.get("resource_usage", {}),
            custom_metrics=result.metadata.get("custom_metrics", {}),
        )

        with self._lock:
            self._metrics.append(metric)
            self._duration_history.append(result.duration)

            # 更新预计算统计
            self._total_executions += 1
            self._cumulative_executions += 1
            if result.success:
                self._total_success += 1
                self._cumulative_success += 1
            else:
                self._total_failures += 1

            if not result.success and result.error:
                error_key = metric.error_type or "unknown"
                self._error_registry[error_key].append({
                    "task_id": result.task_id,
                    "cycle": cycle_number,
                    "message": str(result.error)[:200],
                    "timestamp": metric.timestamp,
                })

        self._log.debug(f"采集执行数据: task={result.task_id}, "
                        f"success={result.success}, duration={result.duration:.3f}s")
        return metric

    def record_cycle_summary(self, summary: CycleSummary) -> None:
        """记录周期汇总"""
        with self._lock:
            self._cycle_summaries.append(summary)
        self._log.debug(f"记录周期汇总: cycle={summary.cycle_number}, "
                        f"success_rate={summary.success_rate:.1%}")

    # ── 统计分析接口 ──

    def get_success_rate(self, last_n: Optional[int] = None) -> float:
        """获取任务成功率"""
        with self._lock:
            metrics = self._metrics[-last_n:] if last_n else self._metrics
            if not metrics:
                return 0.0
            return sum(1 for m in metrics if m.success) / len(metrics)

    def get_average_duration(self, last_n: Optional[int] = None) -> float:
        """获取平均执行时长"""
        with self._lock:
            durations = self._duration_history[-last_n:] if last_n else self._duration_history
            if not durations:
                return 0.0
            return sum(durations) / len(durations)

    def get_error_distribution(self) -> Dict[str, int]:
        """获取错误类型分布"""
        with self._lock:
            return {k: len(v) for k, v in self._error_registry.items()}

    def get_error_frequency_trend(self, error_type: str, window: int = 10) -> List[int]:
        """获取某类错误的频率趋势"""
        with self._lock:
            records = self._error_registry.get(error_type, [])
            if not records:
                return []
            # 按窗口分组统计
            result = []
            for i in range(0, len(records), window):
                result.append(len(records[i:i + window]))
            return result

    def get_cycle_trend(self, metric_name: str = "success_rate",
                        last_n: int = 20) -> List[float]:
        """获取周期级别的指标趋势"""
        with self._lock:
            summaries = self._cycle_summaries[-last_n:]
            if metric_name == "success_rate":
                return [s.success_rate for s in summaries]
            elif metric_name == "avg_duration":
                return [s.avg_duration for s in summaries]
            elif metric_name == "total_errors":
                return [float(s.total_errors) for s in summaries]
            return []

    def get_recent_metrics(self, count: int = 50) -> List[ExecutionMetric]:
        """获取最近的执行指标"""
        with self._lock:
            return self._metrics[-count:]

    def get_total_stats(self) -> Dict[str, Any]:
        """获取全局统计概览（O(1)时间复杂度）"""
        with self._lock:
            return {
                "total_executions": self._cumulative_executions,
                "total_success": self._cumulative_success,
                "total_failures": self._cumulative_executions - self._cumulative_success,
                "overall_success_rate": self._cumulative_success / self._cumulative_executions if self._cumulative_executions > 0 else 0.0,
                "window_executions": self._total_executions,
                "window_success": self._total_success,
                "window_failures": self._total_failures,
                "avg_duration": self.get_average_duration(),
                "total_cycles": len(self._cycle_summaries),
                "error_types_count": len(self._error_registry),
                "latest_cycle": self._cycle_summaries[-1].cycle_number if self._cycle_summaries else 0,
            }

    # ── 数据管理 ──

    def prune_old_data(self, max_cycles: Optional[int] = None) -> int:
        """清理过期数据，返回清理条数"""
        max_cyc = max_cycles if max_cycles is not None else self._retention_cycles
        removed = 0
        with self._lock:
            if self._cycle_summaries:
                cutoff_cycle = self._cycle_summaries[-1].cycle_number - max_cyc
                original_len = len(self._metrics)
                
                # 分离保留和移除的指标
                kept_metrics = [m for m in self._metrics if m.cycle_number > cutoff_cycle]
                removed_metrics = [m for m in self._metrics if m.cycle_number <= cutoff_cycle]
                
                self._metrics = kept_metrics
                removed = original_len - len(self._metrics)
                
                # 重新计算预统计（仅基于保留的数据）
                self._total_executions = len(self._metrics)
                self._total_success = sum(1 for m in self._metrics if m.success)
                self._total_failures = self._total_executions - self._total_success
                
                # 同步清理其他数据
                if len(self._metrics) > 0:
                    self._duration_history = self._duration_history[-len(self._metrics):]
                else:
                    self._duration_history = []
                
                # 同步清理 cycle_summaries（修复：原代码遗漏此项，导致内存持续增长）
                original_summaries = len(self._cycle_summaries)
                self._cycle_summaries = [
                    s for s in self._cycle_summaries if s.cycle_number > cutoff_cycle
                ]
                
                for key in list(self._error_registry.keys()):
                    self._error_registry[key] = [
                        r for r in self._error_registry[key]
                        if r.get("cycle", 0) > cutoff_cycle
                    ]
                    if not self._error_registry[key]:
                        del self._error_registry[key]
        if removed > 0:
            self._log.info(f"清理过期数据: 移除 {removed} 条记录")
        return removed

    def persist_to_file(self, file_path: Optional[str] = None) -> None:
        """持久化采集数据到文件"""
        path = file_path or self._persist_path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            data = {
                "metrics": [
                    {
                        "task_id": m.task_id,
                        "cycle_number": m.cycle_number,
                        "timestamp": m.timestamp,
                        "duration_seconds": m.duration_seconds,
                        "success": m.success,
                        "error_type": m.error_type,
                    }
                    for m in self._metrics
                ],
                "cycle_summaries": [
                    {
                        "cycle_number": s.cycle_number,
                        "total_tasks": s.total_tasks,
                        "completed_tasks": s.completed_tasks,
                        "failed_tasks": s.failed_tasks,
                        "success_rate": s.success_rate,
                        "avg_duration": s.avg_duration,
                    }
                    for s in self._cycle_summaries
                ],
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self._log.debug(f"数据已持久化到 {path}")

    # ── 事件处理 ──

    def _on_event(self, event: SystemEvent) -> None:
        """响应系统事件"""
        if event.event_type == EventType.CYCLE_COMPLETED:
            summary = event.data.get("cycle_summary")
            if summary and isinstance(summary, CycleSummary):
                self.record_cycle_summary(summary)
