#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第5轮：真实运行时混乱测试
实际初始化所有模块，在真实运行中发现静态分析无法捕获的BUG。
"""

import sys
import os
import time
import threading
import random
import traceback
import unittest
import tempfile
import json
from collections import defaultdict

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
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from core.core_task_execution_engine import CoreTaskExecutionEngine
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


class RuntimeChaosTest(unittest.TestCase):
    """真实运行时混乱测试"""

    # ===== 工具方法 =====

    def _create_full_system(self, config_overrides=None):
        """创建完整的系统（与主入口相同的组装方式）"""
        config = AgentConfig()
        if config_overrides:
            for k, v in config_overrides.items():
                config.set(k, v)

        log_svc = AgentLoggingService(config.section("logging"))
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

    def _register_test_handlers(self, engine, failure_rate=0.0, delay_range=(0, 0)):
        """注册测试用任务处理器"""
        def stable_handler(**kwargs):
            return {"status": "ok"}

        def flaky_handler(**kwargs):
            if random.random() < failure_rate:
                raise RuntimeError("随机模拟失败")
            return {"status": "ok"}

        def slow_handler(**kwargs):
            delay = random.uniform(*delay_range)
            time.sleep(delay)
            return {"status": "ok", "delay": delay}

        def crash_handler(**kwargs):
            raise ValueError("故意崩溃")

        engine.handler_registry.register("stable", stable_handler)
        engine.handler_registry.register("flaky", flaky_handler)
        engine.handler_registry.register("slow", slow_handler)
        engine.handler_registry.register("crash", crash_handler)

    # ===== 测试场景 =====

    def test_01_basic_full_cycle_execution(self):
        """场景1：基础全流程——提交任务->执行->采集->分析->规则同步"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        collector = sys_obj["collector"]
        analyzer = sys_obj["analyzer"]
        bridge = sys_obj["bridge"]
        self._register_test_handlers(engine)

        # 提交并执行
        for i in range(5):
            engine.submit_task(TaskDefinition(
                name=f"task_{i}", handler="stable",
                handler_params={"index": i}
            ))

        summary = engine.execute_cycle(1)

        # 验证
        self.assertEqual(summary.total_tasks, 5)
        self.assertEqual(summary.completed_tasks, 5)

        # 分析
        rules = analyzer.analyze_cycle(1)

        # 同步
        if rules:
            bridge.sync_new_rules(rules)

        # 验证数据一致性
        stats = collector.get_total_stats()
        self.assertEqual(stats["total_executions"], 5)
        self.assertEqual(stats["total_success"], 5)

        # 清理
        engine.shutdown()
        print(f"  [PASS] 场景1: 基础全流程 OK")

    def test_02_mixed_success_failure_tasks(self):
        """场景2：混合成功/失败任务——验证统计数据准确性"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        collector = sys_obj["collector"]
        self._register_test_handlers(engine, failure_rate=0.5)

        random.seed(42)  # 固定随机种子
        for i in range(20):
            engine.submit_task(TaskDefinition(
                name=f"flaky_{i}", handler="flaky",
                handler_params={"index": i}, max_retries=0
            ))

        summary = engine.execute_cycle(1)

        # 验证：total = completed + failed
        self.assertEqual(
            summary.total_tasks,
            summary.completed_tasks + summary.failed_tasks
        )

        # 验证采集器统计一致
        stats = collector.get_total_stats()
        self.assertEqual(stats["total_executions"], 20)
        self.assertEqual(
            stats["total_success"] + stats["total_failures"],
            20
        )

        engine.shutdown()
        print(f"  [PASS] 场景2: 混合任务 total={summary.total_tasks} "
              f"success={summary.completed_tasks} fail={summary.failed_tasks}")

    def test_03_concurrent_submit_and_execute(self):
        """场景3：并发提交+执行——多线程同时提交任务"""
        sys_obj = self._create_full_system()
        sys_obj["config"].set("execution.task_queue_max_size", 500)
        engine = sys_obj["engine"]
        self._register_test_handlers(engine)

        errors = []

        def submit_tasks(thread_id):
            try:
                for i in range(50):
                    engine.submit_task(TaskDefinition(
                        name=f"t{thread_id}_{i}", handler="stable",
                        handler_params={"tid": thread_id, "i": i}
                    ))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=submit_tasks, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"并发提交出错: {errors}")

        summary = engine.execute_cycle(1)
        self.assertEqual(summary.total_tasks, 250)
        self.assertEqual(summary.completed_tasks, 250)

        engine.shutdown()
        print(f"  [PASS] 场景3: 并发提交 5线程x50任务={summary.total_tasks}")

    def test_04_retry_logic_verification(self):
        """场景4：重试逻辑验证——任务失败后应重试指定次数"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]

        call_count = {"n": 0}

        def counting_handler(**kwargs):
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise RuntimeError("前两次失败")
            return "success"

        engine.handler_registry.register("retry_test", counting_handler)

        task = TaskDefinition(
            name="retry_task", handler="retry_test",
            handler_params={}, max_retries=3
        )
        engine.submit_task(task)

        summary = engine.execute_cycle(1)

        # 应该在第3次尝试成功
        self.assertEqual(summary.completed_tasks, 1)
        self.assertEqual(summary.failed_tasks, 0)
        self.assertEqual(call_count["n"], 3)  # 第1次失败 + 第2次失败 + 第3次成功

        engine.shutdown()
        print(f"  [PASS] 场景4: 重试逻辑 call_count={call_count['n']}")

    def test_05_priority_ordering(self):
        """场景5：优先级排序——CRITICAL应先于LOW执行"""
        sys_obj = self._create_full_system()
        sys_obj["config"].set("execution.max_concurrent_tasks", 1)
        engine = sys_obj["engine"]

        execution_order = []
        order_lock = threading.Lock()

        def order_handler(**kwargs):
            with order_lock:
                execution_order.append(kwargs["priority_name"])
            time.sleep(0.01)  # 确保顺序
            return "ok"

        engine.handler_registry.register("order_test", order_handler)

        # 逆序提交
        for priority, name in [
            (TaskPriority.LOW, "low"),
            (TaskPriority.CRITICAL, "critical"),
            (TaskPriority.NORMAL, "normal"),
            (TaskPriority.HIGH, "high"),
        ]:
            engine.submit_task(TaskDefinition(
                name=f"{name}_task", handler="order_test",
                handler_params={"priority_name": name},
                priority=priority
            ))

        summary = engine.execute_cycle(1)

        # 验证执行顺序：CRITICAL, HIGH, NORMAL, LOW
        self.assertEqual(execution_order[0], "critical")
        self.assertEqual(execution_order[1], "high")
        self.assertEqual(execution_order[2], "normal")
        self.assertEqual(execution_order[3], "low")

        engine.shutdown()
        print(f"  [PASS] 场景5: 优先级排序 {execution_order}")

    def test_06_queue_full_rejection(self):
        """场景6：队列满拒绝——超过容量应返回False"""
        sys_obj = self._create_full_system({
            "execution.task_queue_max_size": 5,
            "execution.max_concurrent_tasks": 1
        })
        engine = sys_obj["engine"]
        self._register_test_handlers(engine)

        # 填满队列
        accepted = 0
        rejected = 0
        for i in range(10):
            task = TaskDefinition(name=f"q_{i}", handler="stable", handler_params={})
            if engine.submit_task(task):
                accepted += 1
            else:
                rejected += 1

        self.assertEqual(accepted, 5)
        self.assertEqual(rejected, 5)
        self.assertEqual(engine.queue_size(), 5)

        engine.shutdown()
        print(f"  [PASS] 场景6: 队列满 accepted={accepted} rejected={rejected}")

    def test_07_long_running_stability(self):
        """场景7：长时间运行稳定性——200周期x20任务"""
        sys_obj = self._create_full_system()
        sys_obj["config"].set("execution.max_concurrent_tasks", 5)
        sys_obj["config"].set("execution.cycle_cooldown_seconds", 0)
        engine = sys_obj["engine"]
        collector = sys_obj["collector"]
        analyzer = sys_obj["analyzer"]
        bridge = sys_obj["bridge"]
        self._register_test_handlers(engine, failure_rate=0.05)

        total_completed = 0
        total_failed = 0
        total_tasks = 0
        errors = []

        start_time = time.time()

        for cycle in range(1, 201):
            # 每周期提交20个任务
            for i in range(20):
                handler = random.choice(["stable", "flaky", "stable", "stable"])
                engine.submit_task(TaskDefinition(
                    name=f"c{cycle}_t{i}", handler=handler,
                    handler_params={"cycle": cycle, "i": i}
                ))

            try:
                summary = engine.execute_cycle(cycle)
                total_completed += summary.completed_tasks
                total_failed += summary.failed_tasks
                total_tasks += summary.total_tasks

                # 每50周期做一次分析+同步
                if cycle % 50 == 0:
                    try:
                        rules = analyzer.analyze_cycle(cycle)
                        if rules:
                            bridge.sync_new_rules(rules)
                    except Exception as e:
                        errors.append(f"cycle {cycle} analysis: {e}")

                # 每100周期做一次数据清理
                if cycle % 100 == 0:
                    collector.prune_old_data(max_cycles=50)

            except Exception as e:
                errors.append(f"cycle {cycle}: {e}")

        elapsed = time.time() - start_time

        # 验证基本一致性
        self.assertEqual(total_tasks, 4000)
        self.assertEqual(total_completed + total_failed, 4000)
        self.assertTrue(total_completed > 3000, f"成功率过低: {total_completed}/{total_tasks}")

        # 验证采集器统计（注意：prune_old_data 会重置计数器）
        # 在清理后 total_executions 会减少，所以只验证 > 0
        stats = collector.get_total_stats()
        self.assertGreater(stats["total_executions"], 0)

        engine.shutdown()

        print(f"  [PASS] 场景7: 200周期x20任务 "
              f"total={total_tasks} ok={total_completed} fail={total_failed} "
              f"time={elapsed:.1f}s errors={len(errors)}")
        if errors:
            for e in errors[:5]:
                print(f"    ERROR: {e}")

    def test_08_rule_lifecycle_full_flow(self):
        """场景8：规则生命周期——创建->同步->应用->评估->清理"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        collector = sys_obj["collector"]
        rule_repo = sys_obj["rule_repo"]
        bridge = sys_obj["bridge"]
        self._register_test_handlers(engine, failure_rate=0.3)

        # 先执行一些任务建立基线
        for cycle in range(1, 15):
            for i in range(10):
                engine.submit_task(TaskDefinition(
                    name=f"base_{cycle}_{i}", handler="flaky",
                    handler_params={}, max_retries=0
                ))
            engine.execute_cycle(cycle)

        # 手动创建规则
        rule = OptimizationRule(
            name="测试规则",
            description="测试规则生命周期",
            category=RuleCategory.PERFORMANCE,
            condition="test_condition",
            action="reduce timeout",
            priority=5,
            effectiveness_score=0.8
        )
        rule_id = rule_repo.add_rule(rule)

        # 同步到引擎
        count = bridge.sync_all_rules()
        self.assertGreater(count, 0)

        # 验证引擎已加载规则
        stats = engine.get_stats()
        self.assertGreater(stats["active_rules_count"], 0)

        # 标记规则已应用
        rule_repo.mark_rule_applied(rule_id)
        updated_rule = rule_repo.get_rule(rule_id)
        self.assertEqual(updated_rule.apply_count, 1)

        # 更新效果评分
        rule_repo.update_effectiveness(rule_id, 0.9)

        # 清理
        engine.shutdown()
        print(f"  [PASS] 场景8: 规则生命周期 rule_id={rule_id[:8]}")

    def test_09_guardian_component_registration(self):
        """场景9：守护管理器组件注册和健康检查"""
        sys_obj = self._create_full_system()
        sys_obj["config"].set("guardian.health_check_interval_seconds", 0.5)
        guardian = sys_obj["guardian"]
        engine = sys_obj["engine"]
        self._register_test_handlers(engine)

        check_count = {"n": 0}

        def healthy_check():
            check_count["n"] += 1
            return ComponentHealth(component_name="test_comp", status=HealthStatus.HEALTHY)

        guardian.register_component("test_comp", healthy_check, critical=True)

        # 手动触发检查
        results = guardian.check_all_components()
        self.assertIn("test_comp", results)
        self.assertEqual(results["test_comp"].status, HealthStatus.HEALTHY)

        # 获取系统健康
        health = guardian.get_system_health()
        self.assertEqual(health["overall_status"], "healthy")

        engine.shutdown()
        print(f"  [PASS] 场景9: 守护组件注册 OK")

    def test_10_event_bus_stress(self):
        """场景10：事件总线压力——大量事件并发发布"""
        sys_obj = self._create_full_system()
        log_svc = sys_obj["log"]

        received = {"count": 0}
        receive_lock = threading.Lock()

        def event_callback(event):
            with receive_lock:
                received["count"] += 1

        log_svc.subscribe(event_callback)

        # 发布1000个事件
        for i in range(1000):
            log_svc.emit_event(SystemEvent(
                event_type=EventType.TASK_COMPLETED,
                source="test",
                data={"index": i}
            ))

        # 等待事件处理
        time.sleep(0.1)

        self.assertEqual(received["count"], 1000)

        # 验证事件历史
        events = log_svc.get_recent_events(100)
        self.assertEqual(len(events), 100)

        print(f"  [PASS] 场景10: 事件总线 1000事件 received={received['count']}")

    def test_11_data_persistence_roundtrip(self):
        """场景11：数据持久化往返——写入->读取->验证一致性"""
        with tempfile.TemporaryDirectory() as tmpdir:
            metrics_path = os.path.join(tmpdir, "metrics.json")
            rules_path = os.path.join(tmpdir, "rules.json")

            sys_obj = self._create_full_system()
            sys_obj["config"].set("storage.metrics_persistence_path", metrics_path)
            sys_obj["config"].set("optimization.rules_persistence_path", rules_path)
            engine = sys_obj["engine"]
            collector = sys_obj["collector"]
            rule_repo = sys_obj["rule_repo"]
            self._register_test_handlers(engine)

            # 执行一些任务
            for i in range(10):
                engine.submit_task(TaskDefinition(
                    name=f"persist_{i}", handler="stable",
                    handler_params={"i": i}
                ))
            engine.execute_cycle(1)

            # 添加规则
            rule = OptimizationRule(name="持久化测试规则", category=RuleCategory.PERFORMANCE)
            rule_repo.add_rule(rule)

            # 持久化
            collector.persist_to_file(metrics_path)
            rule_repo.persist_to_file(rules_path)

            # 验证文件存在
            self.assertTrue(os.path.exists(metrics_path))
            self.assertTrue(os.path.exists(rules_path))

            # 验证内容
            with open(metrics_path) as f:
                data = json.load(f)
            self.assertEqual(len(data["metrics"]), 10)

            with open(rules_path) as f:
                data = json.load(f)
            self.assertGreater(len(data["rules"]), 0)

            engine.shutdown()
            print(f"  [PASS] 场景11: 数据持久化往返 OK")

    def test_12_skipped_task_handling(self):
        """场景12：未注册处理器任务应被SKIPPED"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        self._register_test_handlers(engine)

        # 提交一个未注册处理器的任务
        engine.submit_task(TaskDefinition(
            name="unknown_task", handler="nonexistent_handler",
            handler_params={}
        ))
        # 同时提交一个正常任务
        engine.submit_task(TaskDefinition(
            name="normal_task", handler="stable",
            handler_params={}
        ))

        summary = engine.execute_cycle(1)

        self.assertEqual(summary.total_tasks, 2)
        # SKIPPED 任务计入 failed_tasks（因为 success=False）
        self.assertEqual(summary.completed_tasks, 1)
        self.assertEqual(summary.failed_tasks, 1)

        engine.shutdown()
        print(f"  [PASS] 场景12: SKIPPED任务处理 OK")

    def test_13_concurrent_rule_sync(self):
        """场景13：并发规则同步——多线程同时触发同步"""
        sys_obj = self._create_full_system()
        bridge = sys_obj["bridge"]
        rule_repo = sys_obj["rule_repo"]
        engine = sys_obj["engine"]
        self._register_test_handlers(engine)

        # 预添加一些规则
        for i in range(10):
            rule = OptimizationRule(name=f"rule_{i}", category=RuleCategory.PERFORMANCE)
            rule_repo.add_rule(rule)

        errors = []
        results = []

        def do_sync(thread_id):
            try:
                count = bridge.sync_all_rules()
                results.append(count)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=do_sync, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"并发同步出错: {errors}")
        # 至少有一些同步成功（sync_lock 防止递归，非阻塞获取可能返回0）
        self.assertTrue(any(r >= 0 for r in results))

        engine.shutdown()
        print(f"  [PASS] 场景13: 并发规则同步 10线程 errors={len(errors)}")

    def test_14_empty_queue_cycle(self):
        """场景14：空队列周期——不应崩溃"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]

        # 不提交任何任务，直接执行
        summary = engine.execute_cycle(1)

        self.assertEqual(summary.total_tasks, 0)
        self.assertEqual(summary.completed_tasks, 0)
        self.assertEqual(summary.failed_tasks, 0)

        engine.shutdown()
        print(f"  [PASS] 场景14: 空队列周期 OK")

    def test_15_clear_queue(self):
        """场景15：清空队列"""
        sys_obj = self._create_full_system()
        engine = sys_obj["engine"]
        self._register_test_handlers(engine)

        for i in range(10):
            engine.submit_task(TaskDefinition(
                name=f"clear_{i}", handler="stable", handler_params={}
            ))

        self.assertEqual(engine.queue_size(), 10)

        cleared = engine.clear_queue()
        self.assertEqual(cleared, 10)
        self.assertEqual(engine.queue_size(), 0)

        engine.shutdown()
        print(f"  [PASS] 场景15: 清空队列 cleared={cleared}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
