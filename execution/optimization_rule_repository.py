"""
优化规则持久化仓库
管理优化规则的创建、查询、更新、持久化与生命周期。
支持文件存储与内存缓存双模式，线程安全。
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from shared.shared_data_models import (
    EventType,
    OptimizationRule,
    RuleCategory,
    SystemEvent,
)
from shared.unified_logging_service import AgentLoggingService


class OptimizationRuleRepository:
    """优化规则仓库：规则的CRUD与持久化管理"""

    def __init__(self, config: Dict[str, Any], logging_service: AgentLoggingService):
        self._config = config
        self._log = logging_service.get_logger("rule_repository")
        self._logging_svc = logging_service

        # 内存存储
        self._rules: Dict[str, OptimizationRule] = {}
        self._lock = threading.RLock()

        # 配置
        self._persist_path = config.get("rules_persistence_path", "./workspace/rules.json")
        self._max_active_rules = config.get("max_active_rules", 50)
        self._effectiveness_threshold = config.get("rule_effectiveness_threshold", 0.3)
        self._auto_deactivate = config.get("auto_deactivate_low_score", True)
        self._deactivate_threshold = config.get("low_score_deactivate_threshold", 0.1)

        # 启动时加载已有规则
        self._load_from_file()

    # ── 规则 CRUD ──

    def add_rule(self, rule: OptimizationRule) -> str:
        """添加新规则"""
        with self._lock:
            self._rules[rule.rule_id] = rule
        self._log.info(f"新增优化规则: [{rule.category.value}] {rule.name} (id={rule.rule_id[:8]})")
        self._logging_svc.emit_event(SystemEvent(
            event_type=EventType.RULE_CREATED,
            source="rule_repository",
            data={"rule_id": rule.rule_id, "rule_name": rule.name, "category": rule.category.value},
        ))
        return rule.rule_id

    def get_rule(self, rule_id: str) -> Optional[OptimizationRule]:
        """按ID获取规则"""
        with self._lock:
            return self._rules.get(rule_id)

    def get_active_rules(self, category: Optional[RuleCategory] = None) -> List[OptimizationRule]:
        """获取所有活跃规则，可按分类筛选"""
        with self._lock:
            rules = [r for r in self._rules.values() if r.is_active]
            if category:
                rules = [r for r in rules if r.category == category]
            return sorted(rules, key=lambda r: r.priority)

    def get_all_rules(self) -> List[OptimizationRule]:
        """获取所有规则（含已停用）"""
        with self._lock:
            return list(self._rules.values())

    def update_rule(self, rule_id: str, **updates: Any) -> bool:
        """更新规则字段"""
        with self._lock:
            rule = self._rules.get(rule_id)
            if not rule:
                return False
            for key, value in updates.items():
                if hasattr(rule, key):
                    setattr(rule, key, value)
        return True

    def deactivate_rule(self, rule_id: str) -> bool:
        """停用规则"""
        return self.update_rule(rule_id, is_active=False)

    def activate_rule(self, rule_id: str) -> bool:
        """重新激活规则"""
        return self.update_rule(rule_id, is_active=True)

    def remove_rule(self, rule_id: str) -> bool:
        """彻底移除规则"""
        with self._lock:
            if rule_id in self._rules:
                del self._rules[rule_id]
                self._log.info(f"移除规则: id={rule_id[:8]}")
                return True
        return False

    # ── 规则应用追踪 ──

    def mark_rule_applied(self, rule_id: str) -> None:
        """标记规则已应用"""
        with self._lock:
            rule = self._rules.get(rule_id)
            if rule:
                rule.applied_at = time.time()
                rule.apply_count += 1

    def update_effectiveness(self, rule_id: str, score: float) -> None:
        """更新规则效果评分"""
        self.update_rule(rule_id, effectiveness_score=score)
        # 自动停用低效规则
        if self._auto_deactivate and score < self._deactivate_threshold:
            self.deactivate_rule(rule_id)
            self._log.warning(f"规则 {rule_id[:8]} 效果评分过低({score:.2f})，已自动停用")

    # ── 规则管理 ──

    def auto_prune_rules(self) -> int:
        """自动清理：停用过多低效规则，保留高效规则"""
        pruned = 0
        with self._lock:
            active_rules = [r for r in self._rules.values() if r.is_active]
            if len(active_rules) <= self._max_active_rules:
                return 0

            # 按效果评分排序，淘汰低分规则
            active_rules.sort(key=lambda r: r.effectiveness_score)
            excess = len(active_rules) - self._max_active_rules
            for rule in active_rules[:excess]:
                rule.is_active = False
                pruned += 1
                self._log.info(f"自动停用低效规则: {rule.name} (score={rule.effectiveness_score:.2f})")

        return pruned

    def get_rules_summary(self) -> Dict[str, Any]:
        """获取规则仓库统计摘要"""
        with self._lock:
            all_rules = list(self._rules.values())
            active = [r for r in all_rules if r.is_active]
            by_category = {}
            for r in active:
                cat = r.category.value
                by_category[cat] = by_category.get(cat, 0) + 1
            return {
                "total_rules": len(all_rules),
                "active_rules": len(active),
                "inactive_rules": len(all_rules) - len(active),
                "by_category": by_category,
                "avg_effectiveness": (
                    sum(r.effectiveness_score for r in active) / len(active)
                    if active else 0.0
                ),
            }

    # ── 持久化 ──

    def persist_to_file(self, file_path: Optional[str] = None) -> None:
        """持久化规则到JSON文件"""
        path = file_path or self._persist_path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            data = {
                "rules": [r.to_dict() for r in self._rules.values()],
                "metadata": {
                    "total": len(self._rules),
                    "persisted_at": time.time(),
                },
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self._log.debug(f"规则已持久化到 {path}")

    def _load_from_file(self) -> None:
        """从文件加载已有规则"""
        path = Path(self._persist_path)
        if not path.exists():
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for rule_data in data.get("rules", []):
                rule = OptimizationRule(
                    rule_id=rule_data["rule_id"],
                    name=rule_data.get("name", ""),
                    description=rule_data.get("description", ""),
                    category=RuleCategory(rule_data.get("category", "performance")),
                    condition=rule_data.get("condition", ""),
                    action=rule_data.get("action", ""),
                    priority=rule_data.get("priority", 10),
                    created_at=rule_data.get("created_at", time.time()),
                    applied_at=rule_data.get("applied_at"),
                    effectiveness_score=rule_data.get("effectiveness_score", 0.0),
                    apply_count=rule_data.get("apply_count", 0),
                    is_active=rule_data.get("is_active", True),
                    source_cycle=rule_data.get("source_cycle", 0),
                )
                self._rules[rule.rule_id] = rule
            self._log.info(f"从文件加载 {len(self._rules)} 条规则")
        except Exception as e:
            self._log.error(f"加载规则文件失败: {e}")
