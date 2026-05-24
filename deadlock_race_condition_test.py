#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第6轮进阶：死锁探测与竞态条件测试
设计可能触发死锁、竞态条件、资源泄漏的场景
"""

import sys
import os
import time
import threading
import random
import gc
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.global_configuration_loader import AgentConfig
from shared.unified_logging_service import AgentLoggingService
from shared.shared_data_models import (
    TaskDefinition, TaskResult, TaskStatus, TaskPriority,
    OptimizationRule, RuleCategory, SystemEvent, EventType,
    ComponentHealth, HealthStatus
)
from execution.execution_data_collector import ExecutionDataCollector
from execution.optimization_rule_repository import OptimizationRuleRepository
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from core.core_task_execution_engine import CoreTaskExecutionEngine
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


class DeadlockAndRaceConditionTest(unittest.TestCase):
    """死锁探测与竞态条件测试"""

    def _create_system(self, config_overrides=None):
        """创建测试系统"""
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

    def test_01_nested_lock_acquisition_order(self):
        """
        场景1：嵌套锁获取顺序测试
        验证不同模块间的锁获取顺序一致，避免死锁
        """
        sys_obj = self._create_system()
        engine = sys_obj["engine"]
        collector = sys_obj["collector"]
        rule_repo = sys_obj["rule_repo"]
        
        errors = []
        success_count = [0]
        
        def operation_a():
            """操作A：先获取规则仓库锁，再获取采集器锁"""
            try:
                for i in range(100):
                    rule = OptimizationRule(name=f"rule_a_{i}")
                    rule_repo.add_rule(rule)
                    collector.get_total_stats()
                success_count[0] += 1
            except Exception as e:
                errors.append(f"A: {e}")
        
        def operation_b():
            """操作B：先获取采集器锁，再获取规则仓库锁"""
            try:
                for i in range(100):
                    collector.get_total_stats()
                    rule = OptimizationRule(name=f"rule_b_{i}")
                    rule_repo.add_rule(rule)
                success_count[0] += 1
            except Exception as e:
                errors.append(f"B: {e}")
        
        # 并发执行两种操作
        threads = []
        for _ in range(10):
            threads.append(threading.Thread(target=operation_a))
            threads.append(threading.Thread(target=operation_b))
        
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)  # 30秒超时
        
        elapsed = time.time() - start
        
        # 验证：无死锁（30秒内完成），无异常
        self.assertEqual(len(errors), 0, f"操作错误: {errors}")
        self.assertEqual(success_count[0], 20)
        self.assertLess(elapsed, 30, f"可能死锁: 耗时{elapsed}秒")
        
        engine.shutdown()
        print(f"  [PASS] 嵌套锁获取顺序: {elapsed:.2f}s, 成功操作={success_count[0]}")

    def test_02_callback_reentrant_lock(self):
        """
        场景2：回调重入锁测试
        事件回调中再次触发事件，验证锁重入正确
        """
        sys_obj = self._create_system()
        log_svc = sys_obj["log"]
        
        call_depth = {"max": 0, "count": 0}
        errors = []
        
        def recursive_callback(event):
            """递归回调：收到事件后再发事件"""
            try:
                call_depth["count"] += 1
                call_depth["max"] = max(call_depth["max"], call_depth["count"])
                
                # 递归深度限制
                depth = event.data.get("depth", 0)
                if depth < 5:
                    log_svc.emit_event(SystemEvent(
                        event_type=EventType.TASK_COMPLETED,
                        source="recursive",
                        data={"depth": depth + 1}
                    ))
                
                call_depth["count"] -= 1
            except Exception as e:
                errors.append(e)
        
        log_svc.subscribe(recursive_callback)
        
        # 发送初始事件
        log_svc.emit_event(SystemEvent(
            event_type=EventType.TASK_COMPLETED,
            source="initial",
            data={"depth": 0}
        ))
        
        time.sleep(0.5)  # 等待所有回调完成
        
        self.assertEqual(len(errors), 0, f"回调错误: {errors}")
        self.assertGreater(call_depth["max"], 0)
        
        print(f"  [PASS] 回调重入锁: 最大深度={call_depth['max']}")

    def test_03_queue_producer_consumer_race(self):
        """
        场景3：队列生产者-消费者竞态测试
        高速生产者和消费者同时操作队列
        """
        sys_obj = self._create_system({"execution.task_queue_max_size": 5000})
        engine = sys_obj["engine"]
        
        produced = [0]
        consumed = [0]
        errors = []
        
        def producer(pid):
            try:
                for i in range(500):
                    task = TaskDefinition(
                        name=f"p{pid}_t{i}",
                        handler="stable",
                        handler_params={}
                    )
                    if engine.submit_task(task):
                        produced[0] += 1
            except Exception as e:
                errors.append(f"producer {pid}: {e}")
        
        # 注册处理器
        def stable_handler(**kwargs):
            return "ok"
        engine.handler_registry.register("stable", stable_handler)
        
        # 启动生产者
        producers = [threading.Thread(target=producer, args=(i,)) for i in range(10)]
        for t in producers:
            t.start()
        
        # 同时执行周期（消费者）
        for cycle in range(1, 11):
            summary = engine.execute_cycle(cycle)
            consumed[0] += summary.completed_tasks
        
        for t in producers:
            t.join()
        
        # 消费剩余
        while engine.queue_size() > 0:
            summary = engine.execute_cycle(100)
            consumed[0] += summary.completed_tasks
        
        self.assertEqual(len(errors), 0, f"生产者错误: {errors}")
        self.assertGreater(produced[0], 0)
        self.assertEqual(produced[0], consumed[0], 
            f"生产-消费不匹配: 生产={produced[0]}, 消费={consumed[0]}")
        
        engine.shutdown()
        print(f"  [PASS] 队列竞态: 生产={produced[0]}, 消费={consumed[0]}")

    def test_04_rule_repository_concurrent_modification(self):
        """
        场景4：规则仓库并发修改测试
        多线程同时添加、更新、删除、查询规则
        """
        sys_obj = self._create_system()
        rule_repo = sys_obj["rule_repo"]
        
        errors = []
        operations = [0]
        
        def add_rules(tid):
            try:
                for i in range(100):
                    rule = OptimizationRule(
                        name=f"rule_{tid}_{i}",
                        category=random.choice(list(RuleCategory))
                    )
                    rule_repo.add_rule(rule)
                    operations[0] += 1
            except Exception as e:
                errors.append(f"add {tid}: {e}")
        
        def update_rules(tid):
            try:
                for i in range(100):
                    rules = rule_repo.get_all_rules()
                    if rules:
                        rule = random.choice(rules)
                        rule_repo.update_rule(rule.rule_id, priority=random.randint(1, 100))
                        operations[0] += 1
            except Exception as e:
                errors.append(f"update {tid}: {e}")
        
        def query_rules(tid):
            try:
                for i in range(100):
                    rule_repo.get_active_rules()
                    rule_repo.get_rules_summary()
                    operations[0] += 1
            except Exception as e:
                errors.append(f"query {tid}: {e}")
        
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=add_rules, args=(i,)))
            threads.append(threading.Thread(target=update_rules, args=(i,)))
            threads.append(threading.Thread(target=query_rules, args=(i,)))
        
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        elapsed = time.time() - start
        
        self.assertEqual(len(errors), 0, f"并发修改错误: {errors}")
        self.assertLess(elapsed, 30, f"可能死锁: {elapsed}秒")
        self.assertGreater(operations[0], 0)
        
        # 验证规则仓库一致性
        all_rules = rule_repo.get_all_rules()
        active_rules = rule_repo.get_active_rules()
        self.assertGreater(len(all_rules), 0)
        self.assertLessEqual(len(active_rules), len(all_rules))
        
        print(f"  [PASS] 规则并发修改: 操作={operations[0]}, 规则={len(all_rules)}")

    def test_05_event_callback_exception_isolation(self):
        """
        场景5：事件回调异常隔离测试
        一个回调抛异常不应影响其他回调
        """
        sys_obj = self._create_system()
        log_svc = sys_obj["log"]
        
        results = {"good": 0, "bad": 0}
        
        def good_callback(event):
            results["good"] += 1
        
        def bad_callback(event):
            results["bad"] += 1
            raise RuntimeError("故意抛异常")
        
        def another_good_callback(event):
            results["good"] += 1
        
        log_svc.subscribe(good_callback)
        log_svc.subscribe(bad_callback)
        log_svc.subscribe(another_good_callback)
        
        # 发送100个事件
        for i in range(100):
            log_svc.emit_event(SystemEvent(
                event_type=EventType.TASK_COMPLETED,
                source="test",
                data={"i": i}
            ))
        
        time.sleep(0.1)
        
        # 验证：good_callback和another_good_callback各执行100次
        # bad_callback执行100次但抛异常
        self.assertEqual(results["good"], 200)  # 两个好回调各100次
        self.assertEqual(results["bad"], 100)   # 坏回调也执行了100次
        
        print(f"  [PASS] 回调异常隔离: 好={results['good']}, 坏={results['bad']}")

    def test_06_guardian_component_state_consistency(self):
        """
        场景6：守护组件状态一致性测试
        并发注册/注销/检查组件
        """
        sys_obj = self._create_system()
        guardian = sys_obj["guardian"]
        
        errors = []
        registered = [0]
        
        def register_components(tid):
            try:
                for i in range(20):
                    name = f"comp_{tid}_{i}"
                    
                    def health_check(n=name):
                        status = random.choice([
                            HealthStatus.HEALTHY,
                            HealthStatus.DEGRADED,
                            HealthStatus.UNHEALTHY
                        ])
                        return ComponentHealth(component_name=n, status=status)
                    
                    guardian.register_component(name, health_check)
                    registered[0] += 1
                    time.sleep(0.001)
                    guardian.unregister_component(name)
            except Exception as e:
                errors.append(f"register {tid}: {e}")
        
        def check_components():
            try:
                for _ in range(50):
                    guardian.check_all_components()
                    time.sleep(0.01)
            except Exception as e:
                errors.append(f"check: {e}")
        
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=register_components, args=(i,)))
        threads.append(threading.Thread(target=check_components))
        
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        elapsed = time.time() - start
        
        self.assertEqual(len(errors), 0, f"守护错误: {errors}")
        self.assertLess(elapsed, 30)
        self.assertGreater(registered[0], 0)
        
        print(f"  [PASS] 守护状态一致性: 注册={registered[0]}")

    def test_07_data_collector_high_frequency_record(self):
        """
        场景7：数据采集器高频记录测试
        极高频率记录任务执行数据
        """
        sys_obj = self._create_system()
        collector = sys_obj["collector"]
        
        errors = []
        record_count = [0]
        
        def record_batch(tid):
            try:
                for i in range(1000):
                    result = TaskResult(
                        task_id=f"task_{tid}_{i}",
                        status=random.choice([TaskStatus.COMPLETED, TaskStatus.FAILED])
                    )
                    result.finished_at = time.time()
                    result.duration_seconds = random.random()
                    if result.status == TaskStatus.FAILED:
                        result.error = "模拟错误"
                    
                    collector.record_task_execution(result, tid * 1000 + i)
                    record_count[0] += 1
            except Exception as e:
                errors.append(f"record {tid}: {e}")
        
        threads = [threading.Thread(target=record_batch, args=(i,)) for i in range(10)]
        
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        elapsed = time.time() - start
        
        self.assertEqual(len(errors), 0, f"记录错误: {errors}")
        self.assertEqual(record_count[0], 10000)
        self.assertLess(elapsed, 30)
        
        # 验证统计一致性
        stats = collector.get_total_stats()
        self.assertEqual(stats["total_executions"], 10000)
        
        print(f"  [PASS] 高频记录: 记录={record_count[0]}, 耗时={elapsed:.2f}s")

    def test_08_sync_bridge_debounce_stress(self):
        """
        场景8：同步桥去抖动压力测试
        高频触发延迟同步
        """
        sys_obj = self._create_system()
        bridge = sys_obj["bridge"]
        rule_repo = sys_obj["rule_repo"]
        engine = sys_obj["engine"]
        
        # 预添加规则
        for i in range(50):
            rule = OptimizationRule(name=f"base_rule_{i}")
            rule_repo.add_rule(rule)
        
        errors = []
        sync_count = [0]
        
        def trigger_sync():
            try:
                for _ in range(100):
                    # 通过事件触发延迟同步
                    bridge._on_rule_created(SystemEvent(
                        event_type=EventType.RULE_CREATED,
                        source="test",
                        data={"rule_id": f"rule_{random.randint(0, 100)}"}
                    ))
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=trigger_sync) for _ in range(10)]
        
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        elapsed = time.time() - start
        
        # 等待所有延迟同步完成
        time.sleep(1)
        
        self.assertEqual(len(errors), 0, f"同步错误: {errors}")
        self.assertLess(elapsed, 30)
        
        engine.shutdown()
        print(f"  [PASS] 去抖动压力: 耗时={elapsed:.2f}s")

    def test_09_executor_shutdown_during_execution(self):
        """
        场景9：执行中关闭执行器测试
        任务执行过程中调用shutdown
        """
        sys_obj = self._create_system({"execution.max_concurrent_tasks": 5})
        engine = sys_obj["engine"]
        
        completed = [0]
        
        def slow_handler(**kwargs):
            time.sleep(0.5)
            return "ok"
        
        engine.handler_registry.register("slow", slow_handler)
        
        # 提交100个慢任务
        for i in range(100):
            engine.submit_task(TaskDefinition(
                name=f"slow_{i}",
                handler="slow",
                handler_params={}
            ))
        
        # 执行一个周期
        summary1 = engine.execute_cycle(1)
        completed[0] += summary1.completed_tasks
        
        # 在执行过程中shutdown
        engine.shutdown()
        
        # 验证：已完成的任务数正确
        self.assertGreater(completed[0], 0)
        
        print(f"  [PASS] 执行中关闭: 完成={completed[0]}")

    def test_10_memory_pressure_with_gc(self):
        """
        场景10：内存压力与GC测试
        大量分配后强制GC，验证无内存泄漏
        """
        sys_obj = self._create_system()
        collector = sys_obj["collector"]
        engine = sys_obj["engine"]
        
        def memory_hungry_handler(**kwargs):
            # 分配大量内存
            data = [list(range(10000)) for _ in range(10)]
            return {"size": len(data)}
        
        engine.handler_registry.register("mem_hungry", memory_hungry_handler)
        
        # 执行大量内存消耗任务
        for i in range(100):
            engine.submit_task(TaskDefinition(
                name=f"mem_{i}",
                handler="mem_hungry",
                handler_params={}
            ))
        
        summary = engine.execute_cycle(1)
        self.assertEqual(summary.completed_tasks, 100)
        
        # 强制GC
        gc.collect()
        
        # 检查循环引用
        gc.collect()
        self.assertEqual(len(gc.garbage), 0, f"内存泄漏: {len(gc.garbage)} 个对象")
        
        engine.shutdown()
        print(f"  [PASS] 内存压力: 完成={summary.completed_tasks}, GC正常")


if __name__ == "__main__":
    unittest.main(verbosity=2)
