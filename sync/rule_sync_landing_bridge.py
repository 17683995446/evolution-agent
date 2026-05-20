"""
规则同步落地桥接器
双向联动模块：将优化层生成的新规则即时同步到执行层，
确保优化成果无缝融入下一轮任务执行。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional

from shared.shared_data_models import (
    EventType,
    OptimizationRule,
    SystemEvent,
)
from core.core_task_execution_engine import CoreTaskExecutionEngine
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from execution.optimization_rule_repository import OptimizationRuleRepository
from shared.unified_logging_service import AgentLoggingService


class SyncStatistics:
    """同步统计"""

    def __init__(self):
        self.total_syncs: int = 0
        self.total_rules_synced: int = 0
        self.last_sync_time: float = 0.0
        self.sync_history: List[Dict[str, Any]] = []


class RuleSyncBridge:
    """
    规则同步桥接器
    - 监听优化层生成的新规则
    - 将规则即时推送到执行引擎
    - 跟踪规则同步状态
    - 提供双向数据流
    """

    def __init__(
        self,
        logging_service: AgentLoggingService,
        rule_repository: OptimizationRuleRepository,
        execution_engine: CoreTaskExecutionEngine,
        optimization_analyzer: IntelligentOptimizationAnalyzer,
    ):
        self._log = logging_service.get_logger("sync_bridge")
        self._logging_svc = logging_service
        self._rules = rule_repository
        self._engine = execution_engine
        self._analyzer = optimization_analyzer

        self._stats = SyncStatistics()
        self._lock = threading.Lock()
        self._syncing = False  # 防止递归同步
        self._sync_pending = False  # 去抖动标志
        self._sync_timer: Optional[threading.Timer] = None

        # 自定义同步钩子
        self._pre_sync_hooks: List[Callable] = []
        self._post_sync_hooks: List[Callable] = []

        # 订阅规则创建事件
        self._logging_svc.subscribe(
            self._on_rule_created,
            filter_types=[EventType.RULE_CREATED],
        )

    # ── 同步接口 ──

    def sync_all_rules(self) -> int:
        """
        将所有活跃规则同步到执行引擎。
        返回同步的规则数量。
        """
        # 防止递归同步（事件回调触发时跳过）
        if self._syncing:
            return 0
        self._syncing = True
        try:
            active_rules = self._rules.get_active_rules()

            # 执行前置钩子
            self._run_hooks(self._pre_sync_hooks, active_rules)

            # 转换为字典列表并推送到执行引擎
            rules_data = [rule.to_dict() for rule in active_rules]
            self._engine.apply_optimization_rules(rules_data)

            # 更新统计
            with self._lock:
                self._stats.total_syncs += 1
                self._stats.total_rules_synced = len(active_rules)
                self._stats.last_sync_time = time.time()
                self._stats.sync_history.append({
                    "sync_time": time.time(),
                    "rules_count": len(active_rules),
                    "rule_ids": [r.rule_id[:8] for r in active_rules],
                })
                # 保留最近100条同步记录
                if len(self._stats.sync_history) > 100:
                    self._stats.sync_history = self._stats.sync_history[-100:]

            # 执行后置钩子
            self._run_hooks(self._post_sync_hooks, active_rules)

            self._log.info(f"规则同步完成: {len(active_rules)} 条活跃规则已推送到执行引擎")
            return len(active_rules)
        finally:
            self._syncing = False

    def sync_new_rules(self, new_rules: List[OptimizationRule]) -> int:
        """仅同步新增的规则"""
        if not new_rules:
            return 0
        # 先确保规则已在仓库中
        for rule in new_rules:
            if not self._rules.get_rule(rule.rule_id):
                self._rules.add_rule(rule)
        # 然后全量同步（确保一致性）
        return self.sync_all_rules()

    def force_resync(self) -> int:
        """强制重新同步所有规则"""
        self._log.info("强制重新同步所有规则")
        return self.sync_all_rules()

    # ── 钩子管理 ──

    def add_pre_sync_hook(self, hook: Callable) -> None:
        """添加同步前钩子"""
        self._pre_sync_hooks.append(hook)

    def add_post_sync_hook(self, hook: Callable) -> None:
        """添加同步后钩子"""
        self._post_sync_hooks.append(hook)

    # ── 状态查询 ──

    def get_sync_stats(self) -> Dict[str, Any]:
        """获取同步统计"""
        with self._lock:
            return {
                "total_syncs": self._stats.total_syncs,
                "total_rules_synced": self._stats.total_rules_synced,
                "last_sync_time": self._stats.last_sync_time,
                "recent_syncs": self._stats.sync_history[-5:],
            }

    def is_rules_applied(self, rule_id: str) -> bool:
        """检查规则是否已应用到执行引擎"""
        engine_rules = self._engine._active_rules_snapshot
        return any(r.get("rule_id") == rule_id for r in engine_rules)

    # ── 事件处理 ──

    def _on_rule_created(self, event: SystemEvent) -> None:
        """响应规则创建事件，使用去抖动机制延迟同步（不阻塞事件线程）"""
        rule_id = event.data.get("rule_id")
        if rule_id:
            self._log.debug(f"检测到新规则创建，安排延迟同步: {rule_id[:8]}")
            self._schedule_delayed_sync()

    def _schedule_delayed_sync(self, delay: float = 0.1) -> None:
        """去抖动：取消之前的定时器，重新设置"""
        if self._sync_timer is not None:
            self._sync_timer.cancel()
        self._sync_timer = threading.Timer(delay, self._do_delayed_sync)
        self._sync_timer.daemon = True
        self._sync_timer.start()

    def _do_delayed_sync(self) -> None:
        """在独立线程中执行延迟同步"""
        try:
            self.sync_all_rules()
        except Exception as e:
            self._log.error(f"延迟同步失败: {e}")

    def _run_hooks(self, hooks: List[Callable], rules: List[OptimizationRule]) -> None:
        """执行钩子函数"""
        for hook in hooks:
            try:
                hook(rules)
            except Exception as e:
                self._log.error(f"同步钩子执行失败: {e}")
