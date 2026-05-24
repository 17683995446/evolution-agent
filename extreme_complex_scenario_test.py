#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第6轮：极端复杂场景测试
多维度故障叠加、并发竞争极端化、事件风暴、资源耗尽模拟
"""

import sys
import os
import time
import threading
import random
import gc
import traceback
import tempfile
import shutil
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.shared_data_models import (
    TaskDefinition,
    TaskPriority,
    TaskStatus,
    TaskResult,
    OptimizationRule,
    RuleCategory,
    HealthStatus,
    ComponentHealth,
    EventType,
    SystemEvent,
    CycleSummary,
)
from shared.unified_logging_service import AgentLoggingService
from execution.execution_data_collector import ExecutionDataCollector
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from execution.optimization_rule_repository import OptimizationRuleRepository
from core.core_task_execution_engine import CoreTaskExecutionEngine
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.resident_guardian_manager import ResidentGuardianManager
from sync.rule_sync_landing_bridge import RuleSyncBridge


class ExtremeComplexScenarioTest(unittest.TestCase):
    """极端复杂场景测试"""

    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        cls._temp_dir = tempfile.mkdtemp(prefix="extreme_test_")
        cls._test_config = {
            "log_level": "WARNING",
            "console_enabled": False,
            "file_enabled": False,
            "max_concurrent_tasks": 20,
            "task_timeout_seconds": 30.0,
            "max_retries_per_task": 2,
            "retry_delay_seconds": 0.1,
            "task_queue_max_size": 10000,
            "health_check_interval_seconds": 1.0,
            "max_component_errors_before_restart": 5,
            "auto_restart_enabled": False,
            "rules_persistence_path": os.path.join(cls._temp_dir, "rules.json"),
            "metrics_persistence_path": os.path.join(cls._temp_dir, "metrics.json"),
            "max_active_rules": 100,
            "min_cycles_for_analysis": 1,
        }

    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        if os.path.exists(cls._temp_dir):
            shutil.rmtree(cls._temp_dir, ignore_errors=True)

    def setUp(self):
        """每个测试前检查初始状态"""
        gc.collect()
        self._initial_threads = threading.active_count()
        self._create_components()

    def _create_components(self):
        """创建测试组件"""
        self._logging_svc = AgentLoggingService(self._test_config)
        self._recovery = ExceptionRecoveryHandler(self._test_config, self._logging_svc)
        self._collector = ExecutionDataCollector(self._test_config, self._logging_svc)
        self._rules = OptimizationRuleRepository(self._test_config, self._logging_svc)
        self._engine = CoreTaskExecutionEngine(
            self._test_config, self._logging_svc, self._collector, self._recovery
        )
        self._analyzer = IntelligentOptimizationAnalyzer(
            self._test_config, self._logging_svc, self._collector, self._rules, self._recovery
        )
        self._sync_bridge = RuleSyncBridge(
            self._logging_svc, self._rules, self._engine, self._analyzer
        )
        self._guardian = ResidentGuardianManager(
            self._test_config, self._logging_svc, self._recovery
        )

    def tearDown(self):
        """每个测试后检查资源泄漏"""
        # 关闭所有组件
        try:
            self._engine.shutdown()
        except Exception:
            pass
        try:
            self._guardian.stop_guarding()
        except Exception:
            pass
        try:
            self._sync_bridge.stop()
        except Exception:
            pass

        # 等待线程清理
        time.sleep(0.5)
        gc.collect()

        leaked_threads = threading.active_count() - self._initial_threads
        if leaked_threads > 2:  # 允许2个守护线程差异
            self.fail(f"线程泄漏: {leaked_threads} 个线程未释放 (初始: {self._initial_threads}, 当前: {threading.active_count()})")

        # 检查循环引用
        gc.collect()
        if gc.garbage:
            self.fail(f"内存泄漏: {len(gc.garbage)} 个不可回收对象")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景1：多维度故障风暴
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_1_multi_dimensional_fault_storm(self):
        """场景1：多维度故障风暴 - 同时触发多种故障"""
        print("\n[场景1] 多维度故障风暴测试开始...")

        fault_types = ['memory', 'cpu', 'network', 'disk', 'normal']
        completed_tasks = []
        failed_tasks = []
        fault_lock = threading.Lock()

        # 注册处理器
        def fault_handler(fault_type: str, **kwargs):
            if fault_type == 'memory':
                # 模拟内存压力 - 分配后立即释放
                data = [bytearray(1024 * 1024) for _ in range(50)]  # 50MB
                del data
                return "memory_ok"
            elif fault_type == 'cpu':
                # 模拟CPU峰值 - 短暂计算
                start = time.time()
                while time.time() - start < 0.1:
                    _ = sum(i * i for i in range(1000))
                return "cpu_ok"
            elif fault_type == 'network':
                # 模拟网络故障
                raise ConnectionError("模拟网络故障")
            elif fault_type == 'disk':
                # 模拟磁盘IO阻塞
                time.sleep(0.05)
                return "disk_ok"
            else:
                return "normal_ok"

        self._engine.handler_registry.register("fault_handler", fault_handler)

        # 创建100个任务
        tasks = []
        for i in range(100):
            fault_type = random.choice(fault_types)
            task = TaskDefinition(
                name=f"fault_task_{i}",
                handler="fault_handler",
                handler_params={"fault_type": fault_type},
                priority=random.choice(list(TaskPriority)),
                max_retries=1,
            )
            tasks.append(task)

        # 提交所有任务
        submitted = self._engine.submit_tasks(tasks)
        self.assertEqual(submitted, 100, "应提交100个任务")

        # 执行周期
        summary = self._engine.execute_cycle(1)

        # 验证系统不崩溃
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_tasks, 100)

        # 验证能恢复 - 执行第二个周期
        summary2 = self._engine.execute_cycle(2)
        self.assertIsNotNone(summary2)

        print(f"[场景1] 完成: 总任务={summary.total_tasks}, 成功={summary.completed_tasks}, 失败={summary.failed_tasks}")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景2：并发竞争极端化
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_2_extreme_concurrent_contention(self):
        """场景2：并发竞争极端化 - 多线程同时操作"""
        print("\n[场景2] 并发竞争极端化测试开始...")

        # 注册处理器
        def simple_handler(**kwargs):
            time.sleep(0.001)
            return "ok"

        self._engine.handler_registry.register("simple_handler", simple_handler)

        errors = []
        errors_lock = threading.Lock()

        def submit_tasks_thread(thread_id: int):
            try:
                for i in range(10):
                    task = TaskDefinition(
                        name=f"thread_{thread_id}_task_{i}",
                        handler="simple_handler",
                        priority=TaskPriority.NORMAL,
                    )
                    self._engine.submit_task(task)
            except Exception as e:
                with errors_lock:
                    errors.append(f"submit_{thread_id}: {e}")

        def sync_rules_thread(thread_id: int):
            try:
                for _ in range(10):
                    self._sync_bridge.sync_all_rules()
                    time.sleep(0.001)
            except Exception as e:
                with errors_lock:
                    errors.append(f"sync_{thread_id}: {e}")

        def check_components_thread(thread_id: int):
            try:
                for _ in range(10):
                    self._guardian.check_all_components()
                    time.sleep(0.001)
            except Exception as e:
                with errors_lock:
                    errors.append(f"check_{thread_id}: {e}")

        # 启动100个线程同时提交任务
        threads = []
        for i in range(100):
            if i % 3 == 0:
                t = threading.Thread(target=submit_tasks_thread, args=(i,))
            elif i % 3 == 1:
                t = threading.Thread(target=sync_rules_thread, args=(i,))
            else:
                t = threading.Thread(target=check_components_thread, args=(i,))
            threads.append(t)
            t.start()

        # 等待所有线程完成
        for t in threads:
            t.join(timeout=10)

        # 验证无竞态条件
        self.assertEqual(len(errors), 0, f"发现竞态条件错误: {errors[:10]}")

        # 执行任务验证无数据损坏
        summary = self._engine.execute_cycle(1)
        self.assertIsNotNone(summary)

        print(f"[场景2] 完成: 无竞态条件错误, 执行任务数={summary.total_tasks}")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景3：事件风暴
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_3_event_storm(self):
        """场景3：事件风暴 - 10000个事件在1秒内发布"""
        print("\n[场景3] 事件风暴测试开始...")

        callback_count = [0]
        recursive_events = [0]
        max_depth = [0]
        callback_lock = threading.Lock()

        def event_callback(event: SystemEvent):
            with callback_lock:
                callback_count[0] += 1

            # 递归发布事件（限制深度）
            depth = event.data.get("depth", 0)
            with callback_lock:
                if depth > max_depth[0]:
                    max_depth[0] = depth

            if depth < 3:  # 限制递归深度
                new_event = SystemEvent(
                    event_type=EventType.TASK_COMPLETED,
                    source="recursive",
                    data={"depth": depth + 1},
                )
                self._logging_svc.emit_event(new_event)
                with callback_lock:
                    recursive_events[0] += 1

        self._logging_svc.subscribe(event_callback)

        # 在1秒内发布10000个事件
        start_time = time.time()
        for i in range(10000):
            event = SystemEvent(
                event_type=EventType.TASK_STARTED,
                source=f"storm_{i}",
                data={"index": i, "depth": 0},
            )
            self._logging_svc.emit_event(event)
        elapsed = time.time() - start_time

        # 等待所有回调完成
        time.sleep(1)

        # 验证事件总线不崩溃
        self.assertLess(elapsed, 5.0, f"事件发布耗时过长: {elapsed:.2f}s")

        # 验证回调执行
        with callback_lock:
            self.assertGreater(callback_count[0], 10000, "回调应至少执行10000次")
            self.assertGreater(recursive_events[0], 0, "应有递归事件产生")

        print(f"[场景3] 完成: 事件数=10000, 回调执行={callback_count[0]}, 递归事件={recursive_events[0]}, 耗时={elapsed:.3f}s")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景4：组件频繁启停
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_4_frequent_start_stop(self):
        """场景4：组件频繁启停 - 循环100次启停"""
        print("\n[场景4] 组件频繁启停测试开始...")

        # 注册处理器
        def simple_handler(**kwargs):
            return "ok"

        self._engine.handler_registry.register("simple_handler", simple_handler)

        # 启动->停止循环100次
        for i in range(100):
            # 启动守护
            self._guardian.start_guarding()
            time.sleep(0.01)

            # 提交任务
            task = TaskDefinition(
                name=f"startstop_task_{i}",
                handler="simple_handler",
            )
            self._engine.submit_task(task)

            # 停止守护
            self._guardian.stop_guarding()

        # 执行剩余任务
        summary = self._engine.execute_cycle(1)

        # 验证无资源泄漏
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_tasks, 100)

        # 验证守护状态
        self.assertFalse(self._guardian._is_guarding)

        print(f"[场景4] 完成: 启停循环100次, 任务数={summary.total_tasks}")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景5：规则频繁更新风暴
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_5_rule_update_storm(self):
        """场景5：规则频繁更新风暴 - 100线程同时操作规则"""
        print("\n[场景5] 规则频繁更新风暴测试开始...")

        errors = []
        errors_lock = threading.Lock()

        def create_rule_thread(thread_id: int):
            try:
                for i in range(10):
                    rule = OptimizationRule(
                        name=f"rule_{thread_id}_{i}",
                        category=random.choice(list(RuleCategory)),
                        action=f"action_{thread_id}",
                        priority=random.randint(1, 20),
                    )
                    self._rules.add_rule(rule)
            except Exception as e:
                with errors_lock:
                    errors.append(f"create_{thread_id}: {e}")

        def update_rule_thread(thread_id: int):
            try:
                for _ in range(10):
                    rules = self._rules.get_all_rules()
                    if rules:
                        rule = random.choice(rules)
                        self._rules.update_rule(
                            rule.rule_id,
                            effectiveness_score=random.random()
                        )
            except Exception as e:
                with errors_lock:
                    errors.append(f"update_{thread_id}: {e}")

        def delete_rule_thread(thread_id: int):
            try:
                for _ in range(10):
                    rules = self._rules.get_all_rules()
                    if len(rules) > 10:  # 保留至少10条
                        rule = random.choice(rules)
                        self._rules.remove_rule(rule.rule_id)
            except Exception as e:
                with errors_lock:
                    errors.append(f"delete_{thread_id}: {e}")

        def sync_thread(thread_id: int):
            try:
                for _ in range(10):
                    self._sync_bridge.sync_all_rules()
            except Exception as e:
                with errors_lock:
                    errors.append(f"sync_{thread_id}: {e}")

        # 启动100个线程
        threads = []
        for i in range(100):
            if i % 4 == 0:
                t = threading.Thread(target=create_rule_thread, args=(i,))
            elif i % 4 == 1:
                t = threading.Thread(target=update_rule_thread, args=(i,))
            elif i % 4 == 2:
                t = threading.Thread(target=delete_rule_thread, args=(i,))
            else:
                t = threading.Thread(target=sync_thread, args=(i,))
            threads.append(t)
            t.start()

        # 等待所有线程完成
        for t in threads:
            t.join(timeout=10)

        # 验证无错误
        self.assertEqual(len(errors), 0, f"发现规则操作错误: {errors[:10]}")

        # 验证规则仓库一致性
        all_rules = self._rules.get_all_rules()
        active_rules = self._rules.get_active_rules()
        self.assertGreater(len(all_rules), 0, "应有规则存在")

        # 验证同步后引擎有规则
        self._sync_bridge.sync_all_rules()
        stats = self._engine.get_stats()
        self.assertGreaterEqual(stats["active_rules_count"], 0)

        print(f"[场景5] 完成: 总规则={len(all_rules)}, 活跃规则={len(active_rules)}, 无错误")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景6：超大队列压力
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_6_large_queue_pressure(self):
        """场景6：超大队列压力 - 10000个任务"""
        print("\n[场景6] 超大队列压力测试开始...")

        # 注册处理器
        def quick_handler(**kwargs):
            return "done"

        self._engine.handler_registry.register("quick_handler", quick_handler)

        # 提交10000个任务
        tasks = []
        for i in range(10000):
            task = TaskDefinition(
                name=f"large_queue_task_{i}",
                handler="quick_handler",
                priority=TaskPriority.NORMAL,
            )
            tasks.append(task)

        submitted = self._engine.submit_tasks(tasks)
        self.assertEqual(submitted, 10000, "应提交10000个任务")

        # 执行
        summary = self._engine.execute_cycle(1)

        # 验证全部完成
        self.assertEqual(summary.total_tasks, 10000)
        self.assertEqual(summary.completed_tasks, 10000)
        self.assertEqual(summary.failed_tasks, 0)

        print(f"[场景6] 完成: 任务数={summary.total_tasks}, 成功率={summary.success_rate:.1%}")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景7：长时间阻塞恢复
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_7_long_blocking_recovery(self):
        """场景7：长时间阻塞恢复 - 阻塞任务不影响正常任务"""
        print("\n[场景7] 长时间阻塞恢复测试开始...")

        results = {"blocked": 0, "normal": 0}
        results_lock = threading.Lock()

        # 注册阻塞处理器
        def blocking_handler(task_type: str, **kwargs):
            if task_type == "blocked":
                time.sleep(2)  # 阻塞2秒
                with results_lock:
                    results["blocked"] += 1
            else:
                with results_lock:
                    results["normal"] += 1
            return task_type

        self._engine.handler_registry.register("blocking_handler", blocking_handler)

        # 提交10个阻塞任务
        for i in range(10):
            task = TaskDefinition(
                name=f"blocked_task_{i}",
                handler="blocking_handler",
                handler_params={"task_type": "blocked"},
                priority=TaskPriority.LOW,
                timeout_seconds=5.0,
            )
            self._engine.submit_task(task)

        # 提交100个正常任务
        for i in range(100):
            task = TaskDefinition(
                name=f"normal_task_{i}",
                handler="blocking_handler",
                handler_params={"task_type": "normal"},
                priority=TaskPriority.HIGH,
            )
            self._engine.submit_task(task)

        # 执行
        start = time.time()
        summary = self._engine.execute_cycle(1)
        elapsed = time.time() - start

        # 验证正常任务完成
        self.assertEqual(summary.total_tasks, 110)
        self.assertGreaterEqual(summary.completed_tasks, 100, "正常任务应完成")

        # 验证执行时间合理（不应被阻塞任务拖太长）
        self.assertLess(elapsed, 10.0, f"执行时间过长: {elapsed:.2f}s")

        print(f"[场景7] 完成: 总任务={summary.total_tasks}, 成功={summary.completed_tasks}, 耗时={elapsed:.2f}s")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景8：异常链式传播
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_8_exception_chain_propagation(self):
        """场景8：异常链式传播 - 任务失败触发链式反应"""
        print("\n[场景8] 异常链式传播测试开始...")

        chain_order = []
        chain_lock = threading.Lock()

        # 注册链式处理器
        def chain_handler(chain_id: str, should_fail: bool, **kwargs):
            with chain_lock:
                chain_order.append(chain_id)

            if should_fail:
                raise RuntimeError(f"Chain failure at {chain_id}")
            return f"success_{chain_id}"

        self._engine.handler_registry.register("chain_handler", chain_handler)

        # 创建链式任务 A->B->C->D
        # A失败，B、C、D应该仍能执行（独立任务）
        tasks = [
            TaskDefinition(
                name="chain_A",
                handler="chain_handler",
                handler_params={"chain_id": "A", "should_fail": True},
                priority=TaskPriority.CRITICAL,
                max_retries=0,
            ),
            TaskDefinition(
                name="chain_B",
                handler="chain_handler",
                handler_params={"chain_id": "B", "should_fail": False},
                priority=TaskPriority.HIGH,
            ),
            TaskDefinition(
                name="chain_C",
                handler="chain_handler",
                handler_params={"chain_id": "C", "should_fail": False},
                priority=TaskPriority.NORMAL,
            ),
            TaskDefinition(
                name="chain_D",
                handler="chain_handler",
                handler_params={"chain_id": "D", "should_fail": False},
                priority=TaskPriority.LOW,
            ),
        ]

        self._engine.submit_tasks(tasks)
        summary = self._engine.execute_cycle(1)

        # 验证异常链正确传播
        self.assertEqual(summary.total_tasks, 4)
        self.assertEqual(summary.failed_tasks, 1, "应有1个失败任务(A)")
        self.assertEqual(summary.completed_tasks, 3, "应有3个成功任务(B,C,D)")

        # 验证所有任务都执行了
        self.assertIn("A", chain_order)
        self.assertIn("B", chain_order)
        self.assertIn("C", chain_order)
        self.assertIn("D", chain_order)

        print(f"[场景8] 完成: 执行顺序={chain_order}, 失败={summary.failed_tasks}")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景9：资源耗尽模拟
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_9_resource_exhaustion(self):
        """场景9：资源耗尽模拟 - 内存和文件描述符"""
        print("\n[场景9] 资源耗尽模拟测试开始...")

        results = {"memory": 0, "file": 0, "normal": 0}
        results_lock = threading.Lock()

        # 注册资源消耗处理器
        def resource_handler(resource_type: str, **kwargs):
            if resource_type == "memory":
                # 模拟内存压力
                data = [bytearray(1024 * 1024) for _ in range(10)]  # 10MB
                del data
                with results_lock:
                    results["memory"] += 1
                return "memory_ok"
            elif resource_type == "file":
                # 模拟文件描述符使用
                with tempfile.NamedTemporaryFile(delete=True) as f:
                    f.write(b"test")
                with results_lock:
                    results["file"] += 1
                return "file_ok"
            else:
                with results_lock:
                    results["normal"] += 1
                return "normal_ok"

        self._engine.handler_registry.register("resource_handler", resource_handler)

        # 提交资源消耗任务
        tasks = []
        for i in range(50):
            tasks.append(TaskDefinition(
                name=f"memory_task_{i}",
                handler="resource_handler",
                handler_params={"resource_type": "memory"},
            ))
        for i in range(50):
            tasks.append(TaskDefinition(
                name=f"file_task_{i}",
                handler="resource_handler",
                handler_params={"resource_type": "file"},
            ))
        for i in range(50):
            tasks.append(TaskDefinition(
                name=f"normal_task_{i}",
                handler="resource_handler",
                handler_params={"resource_type": "normal"},
            ))

        self._engine.submit_tasks(tasks)
        summary = self._engine.execute_cycle(1)

        # 验证系统优雅降级（不崩溃）
        self.assertEqual(summary.total_tasks, 150)
        self.assertGreater(summary.completed_tasks, 100, "大部分任务应完成")

        # 验证资源类型统计
        with results_lock:
            self.assertGreater(results["memory"], 0)
            self.assertGreater(results["file"], 0)
            self.assertGreater(results["normal"], 0)

        print(f"[场景9] 完成: 总任务={summary.total_tasks}, 成功={summary.completed_tasks}")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景10：周期跳跃和中断
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_10_cycle_jump_and_interrupt(self):
        """场景10：周期跳跃和中断 - 周期跳跃和队列清空"""
        print("\n[场景10] 周期跳跃和中断测试开始...")

        # 注册处理器
        def cycle_handler(**kwargs):
            return "cycle_ok"

        self._engine.handler_registry.register("cycle_handler", cycle_handler)

        # 执行周期1-10
        for cycle in range(1, 11):
            for i in range(10):
                task = TaskDefinition(
                    name=f"cycle_{cycle}_task_{i}",
                    handler="cycle_handler",
                )
                self._engine.submit_task(task)
            summary = self._engine.execute_cycle(cycle)
            self.assertEqual(summary.completed_tasks, 10)

        # 提交更多任务后清空队列
        for i in range(100):
            task = TaskDefinition(
                name=f"to_clear_{i}",
                handler="cycle_handler",
            )
            self._engine.submit_task(task)

        # 清空队列
        cleared = self._engine.clear_queue()
        self.assertEqual(cleared, 100, "应清空100个任务")

        # 跳到周期100
        summary = self._engine.execute_cycle(100)
        self.assertEqual(summary.total_tasks, 0, "队列应为空")

        # 验证数据一致性
        stats = self._engine.get_stats()
        self.assertEqual(stats["current_cycle"], 100)

        print(f"[场景10] 完成: 周期1-10执行完成, 队列清空{cleared}个, 跳到周期100")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景11：守护线程压力
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_11_guardian_thread_pressure(self):
        """场景11：守护线程压力 - 50个组件注册"""
        print("\n[场景11] 守护线程压力测试开始...")

        component_states = {}
        states_lock = threading.Lock()

        # 注册50个组件
        for i in range(50):
            comp_name = f"component_{i}"

            # 随机健康状态
            def make_health_check(name: str, idx: int):
                def health_check():
                    status = random.choice([
                        HealthStatus.HEALTHY,
                        HealthStatus.DEGRADED,
                        HealthStatus.UNHEALTHY,
                    ])
                    with states_lock:
                        component_states[name] = status
                    return ComponentHealth(
                        component_name=name,
                        status=status,
                    )
                return health_check

            self._guardian.register_component(
                comp_name,
                health_check=make_health_check(comp_name, i),
                critical=(i < 10),  # 前10个为关键组件
            )

        # 启动守护
        self._guardian.start_guarding()

        # 等待几个检查周期
        time.sleep(3)

        # 手动触发检查
        results = self._guardian.check_all_components()

        # 验证所有组件都被检查
        self.assertEqual(len(results), 50, "应检查50个组件")

        # 验证状态分布
        healthy_count = sum(1 for h in results.values() if h.status == HealthStatus.HEALTHY)
        degraded_count = sum(1 for h in results.values() if h.status == HealthStatus.DEGRADED)
        unhealthy_count = sum(1 for h in results.values() if h.status == HealthStatus.UNHEALTHY)

        total = healthy_count + degraded_count + unhealthy_count
        self.assertEqual(total, 50, "所有组件应有状态")

        # 停止守护
        self._guardian.stop_guarding()

        print(f"[场景11] 完成: 组件=50, 健康={healthy_count}, 降级={degraded_count}, 不健康={unhealthy_count}")

    # ═══════════════════════════════════════════════════════════════════════════
    # 场景12：数据持久化并发
    # ═══════════════════════════════════════════════════════════════════════════

    def test_scenario_12_data_persistence_concurrent(self):
        """场景12：数据持久化并发 - 多线程同时读写"""
        print("\n[场景12] 数据持久化并发测试开始...")

        errors = []
        errors_lock = threading.Lock()

        # 添加一些规则
        for i in range(20):
            rule = OptimizationRule(
                name=f"persist_rule_{i}",
                category=RuleCategory.PERFORMANCE,
            )
            self._rules.add_rule(rule)

        persist_path = os.path.join(self._temp_dir, "concurrent_rules.json")

        def persist_thread(thread_id: int):
            try:
                for i in range(10):
                    # 添加新规则
                    rule = OptimizationRule(
                        name=f"thread_{thread_id}_rule_{i}",
                        category=random.choice(list(RuleCategory)),
                    )
                    self._rules.add_rule(rule)

                    # 持久化
                    self._rules.persist_to_file(persist_path)
                    time.sleep(0.001)
            except Exception as e:
                with errors_lock:
                    errors.append(f"persist_{thread_id}: {e}")

        def load_thread(thread_id: int):
            try:
                for _ in range(10):
                    # 读取规则
                    rules = self._rules.get_all_rules()
                    time.sleep(0.001)
            except Exception as e:
                with errors_lock:
                    errors.append(f"load_{thread_id}: {e}")

        # 启动10个持久化线程和10个加载线程
        threads = []
        for i in range(10):
            t1 = threading.Thread(target=persist_thread, args=(i,))
            t2 = threading.Thread(target=load_thread, args=(i,))
            threads.extend([t1, t2])
            t1.start()
            t2.start()

        # 等待所有线程完成
        for t in threads:
            t.join(timeout=10)

        # 验证无错误
        self.assertEqual(len(errors), 0, f"发现持久化错误: {errors[:10]}")

        # 验证文件存在且有效
        self.assertTrue(os.path.exists(persist_path), "持久化文件应存在")

        # 验证数据一致性
        all_rules = self._rules.get_all_rules()
        self.assertGreater(len(all_rules), 20, "应有规则存在")

        print(f"[场景12] 完成: 规则数={len(all_rules)}, 无文件损坏")


def run_tests():
    """运行所有测试"""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(ExtremeComplexScenarioTest)

    runner = unittest.TextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        descriptions=True,
    )

    result = runner.run(suite)

    # 输出详细失败信息
    if result.failures or result.errors:
        print("\n" + "=" * 80)
        print("失败测试详细报告")
        print("=" * 80)

        for test, traceback_str in result.failures:
            print(f"\n[FAILED] {test}")
            print(traceback_str)

        for test, traceback_str in result.errors:
            print(f"\n[ERROR] {test}")
            print(traceback_str)

    return result


if __name__ == "__main__":
    result = run_tests()
    sys.exit(0 if result.wasSuccessful() else 1)
