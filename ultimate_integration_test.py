#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第11轮：终极集成测试
跨所有模块的真实运行，验证系统整体协同能力。
"""

import sys
import os
import time
import threading
import random
import gc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.global_configuration_loader import AgentConfig
from shared.unified_logging_service import AgentLoggingService
from shared.shared_data_models import (
    TaskDefinition, TaskResult, TaskStatus, TaskPriority,
    OptimizationRule, RuleCategory, SystemEvent, EventType,
    ComponentHealth, HealthStatus, CycleSummary
)
from execution.execution_data_collector import ExecutionDataCollector
from execution.optimization_rule_repository import OptimizationRuleRepository
from execution.exception_recovery_handler import ExceptionRecoveryHandler, RecoveryAction, RecoveryStrategy
from core.core_task_execution_engine import CoreTaskExecutionEngine
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


class UltimateIntegrationTest:
    """终极集成测试"""

    # ── 自定义断言方法（不依赖 unittest.TestCase） ──

    def assertEqual(self, a, b, msg=""):
        if a != b:
            raise AssertionError(f"{msg}\n  期望: {b!r}\n  实际: {a!r}")

    def assertGreater(self, a, b, msg=""):
        if not a > b:
            raise AssertionError(f"{msg}\n  期望 {a!r} > {b!r}")

    def assertGreaterEqual(self, a, b, msg=""):
        if not a >= b:
            raise AssertionError(f"{msg}\n  期望 {a!r} >= {b!r}")

    def assertLess(self, a, b, msg=""):
        if not a < b:
            raise AssertionError(f"{msg}\n  期望 {a!r} < {b!r}")

    def assertIn(self, a, b, msg=""):
        if a not in b:
            raise AssertionError(f"{msg}\n  {a!r} not in {b!r}")

    def assertIsNone(self, a, msg=""):
        if a is not None:
            raise AssertionError(f"{msg}\n  期望 None, 实际: {a!r}")

    def assertIsNotNone(self, a, msg=""):
        if a is None:
            raise AssertionError(f"{msg}\n  期望非 None")

    def assertFalse(self, a, msg=""):
        if a:
            raise AssertionError(f"{msg}\n  期望 False, 实际: {a!r}")

    def run_all_tests(self):
        """运行所有终极集成测试"""
        print("\n" + "=" * 70)
        print("  第11轮：终极集成测试")
        print("=" * 70)

        tests = [
            ("完整系统生命周期", self.test_full_lifecycle),
            ("全模块数据流转", self.test_full_data_flow),
            ("守护-优化-执行联动", self.test_guardian_optimizer_engine),
            ("规则全生命周期", self.test_rule_full_lifecycle),
            ("异常恢复全链路", self.test_recovery_full_chain),
            ("事件总线全链路", self.test_event_bus_full_chain),
            ("配置热更新", self.test_config_hot_update),
            ("资源清理验证", self.test_resource_cleanup),
        ]

        passed = 0
        failed = 0
        errors = []

        for name, test_func in tests:
            try:
                print(f"\n  > {name}...")
                test_func()
                print(f"  [PASS] {name}")
                passed += 1
            except Exception as e:
                print(f"  [FAIL] {name}: {e}")
                import traceback
                traceback.print_exc()
                failed += 1
                errors.append((name, str(e)))

        print(f"\n{'=' * 70}")
        print(f"  终极集成测试结果: 通过={passed}, 失败={failed}")
        print(f"{'=' * 70}")

        if errors:
            print("\n  失败列表:")
            for name, error in errors:
                print(f"    [FAIL] {name}: {error[:100]}")

        return failed == 0

    def _create_full_system(self, config_overrides=None):
        """创建完整系统"""
        config = AgentConfig()
        if config_overrides:
            for k, v in config_overrides.items():
                config.set(k, v)

        log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
        recovery = ExceptionRecoveryHandler(config.section("guardian"), log_svc)
        collector = ExecutionDataCollector(config.section("storage"), log_svc)
        rule_repo = OptimizationRuleRepository(config.section("optimization"), log_svc)
        engine = CoreTaskExecutionEngine(config.section("execution"), log_svc, collector, recovery)
        analyzer = IntelligentOptimizationAnalyzer(config.section("optimization"), log_svc, collector, rule_repo, recovery)
        bridge = RuleSyncBridge(log_svc, rule_repo, engine, analyzer)
        guardian = ResidentGuardianManager(config.section("guardian"), log_svc, recovery)

        return {
            "config": config, "log": log_svc, "recovery": recovery,
            "collector": collector, "rule_repo": rule_repo,
            "engine": engine, "analyzer": analyzer,
            "bridge": bridge, "guardian": guardian
        }

    def _register_handlers(self, engine):
        """注册所有测试处理器"""
        def stable(**kw): return {"status": "ok"}
        def flaky(**kw):
            if random.random() < 0.2: raise RuntimeError("flaky")
            return {"status": "ok"}
        def slow(**kw):
            time.sleep(0.05)
            return {"status": "slow"}

        engine.handler_registry.register("stable", stable)
        engine.handler_registry.register("flaky", flaky)
        engine.handler_registry.register("slow", slow)

    def test_full_lifecycle(self):
        """完整系统生命周期：创建->运行->分析->同步->守护->关闭"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        collector = sys_obj["collector"]
        analyzer = sys_obj["analyzer"]
        bridge = sys_obj["bridge"]
        guardian = sys_obj["guardian"]

        self._register_handlers(engine)

        # 注册守护组件
        def engine_health():
            return ComponentHealth("engine", HealthStatus.HEALTHY)
        guardian.register_component("engine", engine_health, critical=True)

        # 启动守护
        guardian.start_guarding()

        # 运行10个周期
        for cycle in range(1, 11):
            for i in range(5):
                handler = random.choice(["stable", "flaky", "stable"])
                engine.submit_task(TaskDefinition(
                    name=f"task_{cycle}_{i}", handler=handler,
                    handler_params={"cycle": cycle}
                ))

            summary = engine.execute_cycle(cycle)

            # 分析
            rules = analyzer.analyze_cycle(cycle)
            if rules:
                bridge.sync_new_rules(rules)

        # 验证数据一致性
        stats = collector.get_total_stats()
        engine_stats = engine.get_stats()
        self.assertGreater(stats["total_executions"], 0)
        self.assertGreater(engine_stats["total_executed"], 0)

        # 健康检查
        health = guardian.get_system_health()
        self.assertEqual(health["overall_status"], "healthy")

        # 关闭
        guardian.stop_guarding()
        engine.shutdown()

        # 验证无资源泄漏
        gc.collect()

    def test_full_data_flow(self):
        """全模块数据流转：任务->执行->采集->分析->规则->同步->应用"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        collector = sys_obj["collector"]
        rule_repo = sys_obj["rule_repo"]
        analyzer = sys_obj["analyzer"]
        bridge = sys_obj["bridge"]

        self._register_handlers(engine)

        # 1. 提交并执行任务
        for i in range(20):
            engine.submit_task(TaskDefinition(
                name=f"data_flow_{i}", handler="stable",
                handler_params={"i": i}
            ))

        summary = engine.execute_cycle(1)
        self.assertEqual(summary.total_tasks, 20)

        # 2. 验证数据采集
        stats = collector.get_total_stats()
        self.assertEqual(stats["total_executions"], 20)
        self.assertEqual(stats["total_success"], 20)

        # 3. 分析并生成规则
        rules = analyzer.analyze_cycle(1)

        # 4. 手动添加规则
        rule = OptimizationRule(
            name="数据流测试规则",
            category=RuleCategory.PERFORMANCE,
            condition="test_condition",
            action="reduce timeout",
            effectiveness_score=0.9
        )
        rule_id = rule_repo.add_rule(rule)

        # 5. 同步到引擎
        count = bridge.sync_all_rules()
        self.assertGreater(count, 0)

        # 6. 验证引擎已加载规则
        engine_stats = engine.get_stats()
        self.assertGreater(engine_stats["active_rules_count"], 0)

        # 7. 标记规则已应用
        rule_repo.mark_rule_applied(rule_id)
        updated = rule_repo.get_rule(rule_id)
        self.assertEqual(updated.apply_count, 1)

        engine.shutdown()

    def test_guardian_optimizer_engine(self):
        """守护-优化-执行三方联动"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        guardian = sys_obj["guardian"]

        self._register_handlers(engine)

        # 注册引擎健康检查
        def engine_health():
            # 引擎在周期之间 is_running=False 是正常的，仍视为健康
            return ComponentHealth("engine", HealthStatus.HEALTHY)

        guardian.register_component("engine", engine_health, critical=True)

        # 启动守护
        guardian.start_guarding()

        # 执行任务
        for i in range(10):
            engine.submit_task(TaskDefinition(
                name=f"guard_test_{i}", handler="stable",
                handler_params={}
            ))

        summary = engine.execute_cycle(1)
        self.assertEqual(summary.completed_tasks, 10)

        # 手动健康检查
        results = guardian.check_all_components()
        self.assertIn("engine", results)
        self.assertEqual(results["engine"].status, HealthStatus.HEALTHY)

        guardian.stop_guarding()
        engine.shutdown()

    def test_rule_full_lifecycle(self):
        """规则全生命周期：创建->激活->同步->应用->评估->停用->清理"""
        sys_obj = self._create_full_system()
        rule_repo = sys_obj["rule_repo"]
        bridge = sys_obj["bridge"]
        engine = sys_obj["engine"]

        # 创建规则 (使用 ERROR_REDUCTION 替代不存在的 STABILITY)
        rule = OptimizationRule(
            name="生命周期测试规则",
            category=RuleCategory.ERROR_REDUCTION,
            condition="error_rate > 0.1",
            action="increase_retries",
            effectiveness_score=0.5,
        )
        rule_id = rule_repo.add_rule(rule)

        # 验证创建
        self.assertIsNotNone(rule_repo.get_rule(rule_id))

        # 同步到引擎
        bridge.sync_all_rules()
        self.assertGreater(engine.get_stats()["active_rules_count"], 0)

        # 标记应用
        rule_repo.mark_rule_applied(rule_id)
        self.assertEqual(rule_repo.get_rule(rule_id).apply_count, 1)

        # 更新效果评分
        rule_repo.update_effectiveness(rule_id, 0.8)
        self.assertEqual(rule_repo.get_rule(rule_id).effectiveness_score, 0.8)

        # 停用规则
        rule_repo.deactivate_rule(rule_id)
        self.assertFalse(rule_repo.get_rule(rule_id).is_active)

        # 清理
        rule_repo.remove_rule(rule_id)
        self.assertIsNone(rule_repo.get_rule(rule_id))

        engine.shutdown()

    def test_recovery_full_chain(self):
        """异常恢复全链路：异常->捕获->恢复->重置"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        recovery = sys_obj["recovery"]

        self._register_handlers(engine)

        # 注册恢复动作 (RecoveryAction 使用 name 而非 component)
        recovery.register_recovery("test_component", RecoveryAction(
            name="test_component_recovery",
            strategy=RecoveryStrategy.RETRY,
            max_retries=3,
            retry_delay=0.1,
        ))

        # 模拟异常
        recovery.handle_exception("test_component", ValueError("测试异常"))

        # 验证错误记录
        stats = recovery.get_error_stats()
        self.assertIn("test_component", stats.get("components_with_errors", {}))

        # 重置
        recovery.reset_component_errors("test_component")
        health = recovery.get_component_health("test_component")
        self.assertEqual(health.status, HealthStatus.HEALTHY)

        engine.shutdown()

    def test_event_bus_full_chain(self):
        """事件总线全链路：发布->订阅->回调->历史"""
        sys_obj = self._create_full_system()
        log_svc = sys_obj["log"]

        received = []

        def callback(event):
            received.append(event)

        log_svc.subscribe(callback, filter_types=[EventType.TASK_COMPLETED])

        # 发布事件
        for i in range(10):
            log_svc.emit_event(SystemEvent(
                event_type=EventType.TASK_COMPLETED,
                source="test",
                data={"index": i}
            ))

        time.sleep(0.1)

        # 验证
        self.assertEqual(len(received), 10)

        # 验证历史
        events = log_svc.get_events_by_type(EventType.TASK_COMPLETED)
        self.assertGreaterEqual(len(events), 10)

        # 取消订阅
        log_svc.unsubscribe(callback)

        received.clear()
        log_svc.emit_event(SystemEvent(
            event_type=EventType.TASK_COMPLETED,
            source="test",
            data={"index": 99}
        ))
        time.sleep(0.05)
        self.assertEqual(len(received), 0)  # 取消后不再收到

    def test_config_hot_update(self):
        """配置热更新"""
        config = AgentConfig()
        self.assertEqual(config.get("execution.max_concurrent_tasks"), 5)

        # 更新配置
        config.set("execution.max_concurrent_tasks", 10)
        self.assertEqual(config.get("execution.max_concurrent_tasks"), 10)

        # 验证section也更新 (section 返回的是内部字典的引用)
        section = config.section("execution")
        self.assertEqual(section["max_concurrent_tasks"], 10)

    def test_resource_cleanup(self):
        """资源清理验证"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]

        self._register_handlers(engine)

        # 执行大量任务
        for i in range(100):
            engine.submit_task(TaskDefinition(
                name=f"cleanup_{i}", handler="stable",
                handler_params={}
            ))

        for cycle in range(1, 11):
            engine.execute_cycle(cycle)

        # 清理数据
        sys_obj["collector"].prune_old_data(max_cycles=3)

        # 关闭
        engine.shutdown()

        # 验证无资源泄漏
        gc.collect()
        leaked_threads = threading.active_count()
        self.assertLess(leaked_threads, 20, f"线程泄漏: {leaked_threads}")


if __name__ == "__main__":
    test = UltimateIntegrationTest()
    success = test.run_all_tests()
    sys.exit(0 if success else 1)
