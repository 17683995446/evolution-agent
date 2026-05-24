"""
智能优化分析器
副线优化模块：实时复盘执行数据，识别短板、生成优化规则、评估规则效果。
基于统计分析和启发式规则，持续打磨执行流程。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

from shared.shared_data_models import (
    EventType,
    OptimizationRule,
    RuleCategory,
    SystemEvent,
)
from execution.execution_data_collector import ExecutionDataCollector
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from execution.optimization_rule_repository import OptimizationRuleRepository
from shared.unified_logging_service import AgentLoggingService


class OptimizationInsight:
    """单条优化洞察"""

    def __init__(self, category: RuleCategory, name: str,
                 description: str, condition: str, action: str,
                 priority: int = 10, confidence: float = 0.5):
        self.category = category
        self.name = name
        self.description = description
        self.condition = condition
        self.action = action
        self.priority = priority
        self.confidence = confidence


class IntelligentOptimizationAnalyzer:
    """
    智能优化分析器
    - 分析执行数据，识别效率瓶颈
    - 生成优化规则
    - 评估已有规则的效果
    - 提供优化建议
    """

    def __init__(
        self,
        config: Dict[str, Any],
        logging_service: AgentLoggingService,
        data_collector: ExecutionDataCollector,
        rule_repository: OptimizationRuleRepository,
        recovery_handler: ExceptionRecoveryHandler,
    ):
        self._config = config
        self._log = logging_service.get_logger("optimization_analyzer")
        self._logging_svc = logging_service
        self._collector = data_collector
        self._rules = rule_repository
        self._recovery = recovery_handler

        # 分析状态
        self._analysis_count: int = 0
        self._last_analysis_time: float = 0.0
        self._lock = threading.RLock()

        # 配置
        self._min_cycles = config.get("min_cycles_for_analysis", 1)
        self._effectiveness_threshold = config.get("rule_effectiveness_threshold", 0.3)

        # 历史基线（用于趋势对比）
        self._baseline_success_rate: float = 0.0
        self._baseline_avg_duration: float = 0.0
        self._baseline_established: bool = False

    # ── 核心分析接口 ──

    def analyze_cycle(self, cycle_number: int) -> List[OptimizationRule]:
        """
        分析一个执行周期，返回新生成的优化规则列表。
        这是副线优化的核心调用。
        """
        with self._lock:
            self._analysis_count += 1
            self._last_analysis_time = time.time()

        stats = self._collector.get_total_stats()
        if stats["total_executions"] == 0:
            self._log.debug(f"周期 {cycle_number}: 无执行数据，跳过分析")
            return []

        self._log.info(f"─── 优化分析周期 {cycle_number} 开始 ───")

        # 采集分析数据
        insights = self._run_analysis(cycle_number, stats)

        # 将洞察转化为规则（在锁保护下避免TOCTOU竞态）
        new_rules = []
        with self._lock:
            for insight in insights:
                rule = self._insight_to_rule(insight, cycle_number)
                if rule:
                    # 检查是否已有类似规则
                    if not self._has_similar_rule(rule):
                        self._rules.add_rule(rule)
                        new_rules.append(rule)
                        self._log.info(f"生成优化规则: [{rule.category.value}] {rule.name}")
                    else:
                        self._log.debug(f"已存在类似规则，跳过: {rule.name}")

        # 评估已有规则效果
        self._evaluate_existing_rules(cycle_number, stats)

        # 更新基线
        self._update_baseline(stats)

        # 清理低效规则
        pruned = self._rules.auto_prune_rules()

        self._log.info(
            f"─── 优化分析周期 {cycle_number} 完成 ─── "
            f"新规则={len(new_rules)}, 总活跃规则={len(self._rules.get_active_rules())}, "
            f"清理={pruned}"
        )

        return new_rules

    def get_analysis_stats(self) -> Dict[str, Any]:
        """获取分析器统计"""
        with self._lock:
            return {
                "total_analyses": self._analysis_count,
                "last_analysis_time": self._last_analysis_time,
                "baseline_established": self._baseline_established,
                "baseline_success_rate": self._baseline_success_rate,
                "baseline_avg_duration": self._baseline_avg_duration,
            }

    def start(self) -> None:
        """启动优化分析器"""
        self._log.info("优化分析器已启动")

    def stop(self) -> None:
        """停止优化分析器"""
        self._log.info("优化分析器已停止")

    # ── 分析策略 ──

    def _run_analysis(self, cycle_number: int, stats: Dict[str, Any]) -> List[OptimizationInsight]:
        """运行所有分析策略，收集优化洞察"""
        insights: List[OptimizationInsight] = []

        # 策略1: 成功率分析
        insights.extend(self._analyze_success_rate(stats))

        # 策略2: 执行耗时分析
        insights.extend(self._analyze_duration())

        # 策略3: 错误模式分析
        insights.extend(self._analyze_error_patterns())

        # 策略4: 资源使用分析
        insights.extend(self._analyze_resource_usage())

        # 策略5: 趋势分析
        insights.extend(self._analyze_trends())

        return insights

    def _analyze_success_rate(self, stats: Dict[str, Any]) -> List[OptimizationInsight]:
        """分析成功率，识别质量下降"""
        insights = []
        current_rate = stats["overall_success_rate"]

        with self._lock:
            baseline_established = self._baseline_established
            baseline_success_rate = self._baseline_success_rate

        if baseline_established:
            # 与基线对比
            if current_rate < baseline_success_rate - 0.1:
                insights.append(OptimizationInsight(
                    category=RuleCategory.QUALITY_IMPROVEMENT,
                    name="成功率下降告警",
                    description=f"当前成功率({current_rate:.1%})显著低于基线({baseline_success_rate:.1%})",
                    condition=f"success_rate < {baseline_success_rate - 0.05:.2f}",
                    action="增加任务前置校验步骤，失败任务快速重试",
                    priority=3,
                    confidence=0.8,
                ))
        elif current_rate < 0.8 and stats["total_executions"] >= 5:
            insights.append(OptimizationInsight(
                category=RuleCategory.ERROR_REDUCTION,
                name="低成功率优化",
                description=f"成功率({current_rate:.1%})偏低，需要排查错误原因",
                condition="success_rate < 0.8",
                action="分析高频错误类型，针对性添加容错处理",
                priority=2,
                confidence=0.9,
            ))

        return insights

    def _analyze_duration(self) -> List[OptimizationInsight]:
        """分析执行耗时，识别性能瓶颈"""
        insights = []
        avg_duration = self._collector.get_average_duration()

        with self._lock:
            baseline_established = self._baseline_established
            baseline_avg_duration = self._baseline_avg_duration

        if baseline_established and avg_duration > baseline_avg_duration * 1.5:
            insights.append(OptimizationInsight(
                category=RuleCategory.PERFORMANCE,
                name="执行耗时增长告警",
                description=f"平均耗时({avg_duration:.3f}s)显著高于基线({baseline_avg_duration:.3f}s)",
                condition=f"avg_duration > {baseline_avg_duration * 1.3:.3f}",
                action="检查是否存在资源竞争或外部依赖延迟，考虑增加并发度",
                priority=4,
                confidence=0.7,
            ))

        # 检查最近是否有特别慢的任务
        recent = self._collector.get_recent_metrics(20)
        if recent:
            slow_tasks = [m for m in recent if m.duration_seconds > avg_duration * 3 and avg_duration > 0]
            if len(slow_tasks) >= 3:
                insights.append(OptimizationInsight(
                    category=RuleCategory.PERFORMANCE,
                    name="慢任务检测",
                    description=f"发现{len(slow_tasks)}个异常慢任务",
                    condition="task_duration > avg_duration * 3",
                    action="对慢任务添加超时熔断机制，避免拖慢整体进度",
                    priority=5,
                    confidence=0.75,
                ))

        return insights

    def _analyze_error_patterns(self) -> List[OptimizationInsight]:
        """分析错误模式，识别系统性问题"""
        insights = []
        error_dist = self._collector.get_error_distribution()

        if not error_dist:
            return insights

        # 找出最高频的错误类型
        top_error = max(error_dist, key=error_dist.get)
        top_count = error_dist[top_error]

        if top_count >= 3:
            insights.append(OptimizationInsight(
                category=RuleCategory.ERROR_REDUCTION,
                name=f"高频错误: {top_error}",
                description=f"错误类型'{top_error}'出现{top_count}次，需要重点关注",
                condition=f"error_type={top_error} count >= 3",
                action=f"为'{top_error}'添加专门的异常捕获和恢复逻辑",
                priority=2,
                confidence=0.85,
            ))

        # 检查错误是否在增长
        if top_error:
            trend = self._collector.get_error_frequency_trend(top_error, window=5)
            if len(trend) >= 2 and trend[-2] > 0 and trend[-1] > trend[-2] * 1.5:
                insights.append(OptimizationInsight(
                    category=RuleCategory.ERROR_REDUCTION,
                    name=f"错误增长趋势: {top_error}",
                    description=f"'{top_error}'错误频率呈上升趋势",
                    condition=f"{top_error} frequency increasing",
                    action="紧急排查根因，考虑临时降级或跳过相关任务",
                    priority=1,
                    confidence=0.9,
                ))

        return insights

    def _analyze_resource_usage(self) -> List[OptimizationInsight]:
        """分析资源使用情况"""
        insights = []
        recent = self._collector.get_recent_metrics(10)

        # 检查是否有资源使用异常高的任务
        high_resource_tasks = []
        for m in recent:
            for resource, value in m.resource_usage.items():
                if isinstance(value, (int, float)) and value > 80:  # 假设80%为高使用率
                    high_resource_tasks.append((m.task_id, resource, value))

        if len(high_resource_tasks) >= 2:
            insights.append(OptimizationInsight(
                category=RuleCategory.RESOURCE_SAVING,
                name="资源使用偏高",
                description=f"发现{len(high_resource_tasks)}个高资源使用记录",
                condition="resource_usage > 80%",
                action="优化资源密集型任务的执行策略，考虑分批处理",
                priority=6,
                confidence=0.6,
            ))

        return insights

    def _analyze_trends(self) -> List[OptimizationInsight]:
        """分析整体趋势"""
        insights = []
        success_trend = self._collector.get_cycle_trend("success_rate", last_n=10)

        if len(success_trend) >= 5:
            # 检查是否持续下降
            recent_avg = sum(success_trend[-3:]) / 3
            earlier_avg = sum(success_trend[:3]) / 3
            if recent_avg < earlier_avg - 0.1:
                insights.append(OptimizationInsight(
                    category=RuleCategory.FLOW_OPTIMIZATION,
                    name="成功率下降趋势",
                    description=f"近期成功率({recent_avg:.1%})低于早期({earlier_avg:.1%})",
                    condition="success_rate trend declining",
                    action="全面审查执行流程，识别引入的退化因素",
                    priority=3,
                    confidence=0.7,
                ))

            # 检查是否持续提升（正向反馈）
            if recent_avg > earlier_avg + 0.1:
                self._log.info(f"正向趋势: 成功率从{earlier_avg:.1%}提升到{recent_avg:.1%}")

        return insights

    # ── 规则管理 ──

    def _insight_to_rule(self, insight: OptimizationInsight,
                         cycle_number: int) -> Optional[OptimizationRule]:
        """将优化洞察转化为正式规则"""
        if insight.confidence < 0.4:
            return None
        return OptimizationRule(
            name=insight.name,
            description=insight.description,
            category=insight.category,
            condition=insight.condition,
            action=insight.action,
            priority=insight.priority,
            effectiveness_score=insight.confidence,
            source_cycle=cycle_number,
        )

    def _has_similar_rule(self, new_rule: OptimizationRule) -> bool:
        """检查是否已存在类似规则"""
        existing = self._rules.get_active_rules()
        for rule in existing:
            if rule.name == new_rule.name:
                return True
            if rule.category == new_rule.category and rule.condition == new_rule.condition:
                return True
        return False

    def _evaluate_existing_rules(self, cycle_number: int, stats: Dict[str, Any]) -> None:
        """评估已有规则的效果"""
        active_rules = self._rules.get_active_rules()
        for rule in active_rules:
            # 简单效果评估：如果成功率在提升，给所有活跃规则加分
            with self._lock:
                baseline_established = self._baseline_established
                baseline_success_rate = self._baseline_success_rate
            if baseline_established:
                improvement = stats["overall_success_rate"] - baseline_success_rate
                if improvement > 0:
                    new_score = min(1.0, rule.effectiveness_score + improvement * 0.1)
                    self._rules.update_effectiveness(rule.rule_id, new_score)

    def _update_baseline(self, stats: Dict[str, Any]) -> None:
        """更新性能基线"""
        with self._lock:
            if not self._baseline_established and stats["total_executions"] >= 10:
                self._baseline_success_rate = stats["overall_success_rate"]
                self._baseline_avg_duration = stats["avg_duration"]
                self._baseline_established = True
                self._log.info(
                    f"性能基线已建立: 成功率={self._baseline_success_rate:.1%}, "
                    f"平均耗时={self._baseline_avg_duration:.3f}s"
                )

            # 定期更新基线（每20次分析）
            if self._baseline_established and self._analysis_count % 20 == 0:
                self._baseline_success_rate = (
                    self._baseline_success_rate * 0.7 + stats["overall_success_rate"] * 0.3
                )
                self._baseline_avg_duration = (
                    self._baseline_avg_duration * 0.7 + stats["avg_duration"] * 0.3
                )
                self._log.debug(f"性能基线已更新: 成功率={self._baseline_success_rate:.1%}")
