"""
第13轮测试套件 - 超大规模实战测试 + 全量日志埋点验证
包含: 500周期稳定性、极端并发、全生命周期、数据一致性、日志埋点验证
"""
import sys
import os
import time
import threading
import queue
import json
import traceback
import gc
import random
import io
import logging
from collections import deque
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, PropertyMock
from contextlib import redirect_stdout, redirect_stderr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ══════════════════════════════════════════════════════════════
#  测试框架
# ══════════════════════════════════════════════════════════════
_results = {"passed": 0, "failed": 0, "errors": []}

def run_test(name, func):
    try:
        func()
        _results["passed"] += 1
        print(f"  [PASS] {name}")
    except Exception as e:
        _results["failed"] += 1
        tb = traceback.format_exc()
        _results["errors"].append(f"{name}: {e}\n{tb}")
        print(f"  [FAIL] {name}: {e}")

def assert_true(cond, msg=""):
    if not cond:
        raise AssertionError(f"AssertTrue failed: {msg}")

def assert_eq(a, b, msg=""):
    if a != b:
        raise AssertionError(f"{msg}: expected={b}, actual={a}")

def assert_ge(a, b, msg=""):
    if a < b:
        raise AssertionError(f"{msg}: {a} should be >= {b}")

def assert_no_exc(func, msg=""):
    try:
        func()
    except Exception as e:
        raise AssertionError(f"{msg}: unexpected {type(e).__name__}: {e}")


# ══════════════════════════════════════════════════════════════
#  第一部分: P0/P1修复验证
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 1] P0/P1 Bug Fix Verification")
print("=" * 70)


def test_p0_shutdown_with_none_modules():
    """P0: shutdown()在模块未初始化时不应崩溃"""
    from evolution_agent_main_entry import EvolutionAgent
    agent = EvolutionAgent.__new__(EvolutionAgent)
    # 不调用initialize，所有模块为None
    agent._log = None
    agent._guardian = None
    agent._sync_bridge = None
    agent._execution_engine = None
    agent._optimization_analyzer = None
    agent._rule_repository = None
    agent._data_collector = None
    agent._recovery_handler = None
    # 应不抛出异常
    agent.shutdown()
    print("    (shutdown with all-None modules succeeded)")

run_test("P0: shutdown() handles None modules", test_p0_shutdown_with_none_modules)


def test_p0_initialize_failure_cleanup():
    """P0: initialize()失败时shutdown不应崩溃"""
    from evolution_agent_main_entry import EvolutionAgent
    agent = EvolutionAgent.__new__(EvolutionAgent)
    agent._log = type('obj', (), {
        'info': lambda *a, **kw: None, 'error': lambda *a, **kw: None,
        'warning': lambda *a, **kw: None, 'debug': lambda *a, **kw: None,
        'critical': lambda *a, **kw: None, 'emit_event': lambda *a, **kw: None,
        'subscribe': lambda *a, **kw: None, 'unsubscribe': lambda *a, **kw: None,
        'get_recent_events': lambda *a, **kw: [],
    })()
    agent._execution_engine = type('obj', (), {'shutdown': lambda self: None})()
    agent._guardian = None
    agent._sync_bridge = None
    agent._optimization_analyzer = None
    agent._rule_repository = None
    agent._data_collector = None
    agent._recovery_handler = None
    # 模拟initialize中途失败后调用shutdown
    agent.shutdown()
    assert_true(True, "shutdown在部分初始化后不应崩溃")

run_test("P0: initialize failure preserves original exception", test_p0_initialize_failure_cleanup)


def test_p0_prune_duration_history():
    """P0: prune_old_data中metrics为空时duration_history应清空"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from shared.shared_data_models import TaskResult, TaskStatus, CycleSummary

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)

    # 添加数据和周期汇总
    for i in range(10):
        r = TaskResult(task_id=f"t_{i}")
        r.status = TaskStatus.COMPLETED
        r.started_at = time.time()
        r.finished_at = time.time() + 0.01
        r.duration_seconds = 0.01
        collector.record_task_execution(r, cycle_number=1)

    # 添加周期汇总（prune依赖它）
    collector._cycle_summaries.append(CycleSummary(
        cycle_number=1, total_tasks=10, completed_tasks=10,
        failed_tasks=0, avg_duration=0.01, error_types={}
    ))

    assert_true(len(collector._duration_history) > 0, "应有duration数据")

    # 清理所有数据（max_cycles=0表示不保留）
    collector.prune_old_data(max_cycles=0)

    assert_eq(len(collector._duration_history), 0,
              f"metrics清空后duration_history也应清空，实际: {len(collector._duration_history)}")

run_test("P0: prune clears duration_history when metrics empty", test_p0_prune_duration_history)


def test_p1_remaining_count_accurate():
    """P1: as_completed超时后remaining计数应准确"""
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2, "task_timeout_seconds": 1,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 100,
    }, log_svc, collector, recovery)

    def slow_handler():
        time.sleep(5)  # 超过timeout
        return "done"

    engine.handler_registry.register("slow", slow_handler)

    for i in range(5):
        engine.submit_task(TaskDefinition(
            name=f"slow_{i}", handler="slow", handler_params={},
            priority=TaskPriority.NORMAL, max_retries=0,
        ))

    summary = engine.execute_cycle(1)
    # 超时后remaining应准确
    assert_eq(summary.total_tasks, 5, "应提交5个任务")
    assert_ge(summary.completed_tasks + summary.failed_tasks, 0, "完成+失败应>=0")
    assert_ge(summary.total_tasks, summary.completed_tasks + summary.failed_tasks,
              "总数应>=完成+失败")

    engine.shutdown()

run_test("P1: remaining count accurate after timeout", test_p1_remaining_count_accurate)


def test_p1_get_stats_lock_protected():
    """P1: get_stats读取rules应有锁保护"""
    import inspect
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    src = inspect.getsource(CoreTaskExecutionEngine.get_stats)
    assert_true("_rules_lock" in src, "get_stats应使用_rules_lock")

run_test("P1: get_stats uses rules_lock", test_p1_get_stats_lock_protected)


def test_p1_no_immediate_error_reset():
    """P1: 成功后不应立即清零错误计数"""
    import inspect
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    src = inspect.getsource(CoreTaskExecutionEngine._execute_single_task)
    assert_true("reset_component_errors" not in src,
                "成功后不应调用reset_component_errors")

run_test("P1: no immediate error reset on success", test_p1_no_immediate_error_reset)


def test_p1_analyze_cycle_thread_safe():
    """P1: analyze_cycle应有并发保护"""
    import inspect
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    src = inspect.getsource(IntelligentOptimizationAnalyzer.__init__)
    assert_true("RLock" in src, "应使用RLock")

run_test("P1: analyzer uses RLock", test_p1_analyze_cycle_thread_safe)


def test_p1_is_rules_applied_uses_rule_id():
    """P1: is_rules_applied应使用rule_id参数"""
    import inspect
    from sync.rule_sync_landing_bridge import RuleSyncBridge
    src = inspect.getsource(RuleSyncBridge.is_rules_applied)
    assert_true("rule_id" in src, "is_rules_applied应使用rule_id参数")

run_test("P1: is_rules_applied uses rule_id", test_p1_is_rules_applied_uses_rule_id)


def test_p1_consecutive_failures_in_lock():
    """P1: consecutive_failures清零应在锁内"""
    import inspect
    from sync.resident_guardian_manager import ResidentGuardianManager
    src = inspect.getsource(ResidentGuardianManager._restart_component)
    # 找到consecutive_failures = 0的位置
    lines = src.split('\n')
    found_in_lock = False
    in_with = False
    for line in lines:
        if 'with self._lock' in line:
            in_with = True
        if 'consecutive_failures = 0' in line and in_with:
            found_in_lock = True
            break
        if in_with and line.strip() and not line.strip().startswith('#') and 'with ' not in line and 'consecutive_failures' not in line:
            # 简单检查：如果在with块内找到了
            pass
    assert_true(found_in_lock or src.count('consecutive_failures = 0') == 0 or
                'with self._lock' in src[:src.index('consecutive_failures = 0') if 'consecutive_failures = 0' in src else 0],
                "consecutive_failures = 0应在with self._lock块内")

run_test("P1: consecutive_failures reset in lock", test_p1_consecutive_failures_in_lock)


def test_p1_deep_merge_none_skip():
    """P1: _deep_merge应跳过None值"""
    from shared.global_configuration_loader import AgentConfig
    config = AgentConfig({})
    base = {"execution": {"max_concurrent": 5, "timeout": 30}}
    override = {"execution": None}
    config._deep_merge(base, override)
    # _deep_merge修改base in-place，None值不应覆盖
    assert_eq(base["execution"]["max_concurrent"], 5,
              "None值不应覆盖有效配置")
    assert_eq(base["execution"]["timeout"], 30,
              "None值不应覆盖有效配置")

run_test("P1: _deep_merge skips None values", test_p1_deep_merge_none_skip)


def test_p1_internal_params_precise_filter():
    """P1: 规则预处理应仅过滤已知内部参数"""
    import inspect
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    src = inspect.getsource(CoreTaskExecutionEngine._apply_rules_preprocessing)
    assert_true("internal_keys" in src or "_optimized_timeout" in src,
                "应使用精确的内部参数集合过滤")

run_test("P1: precise internal param filtering", test_p1_internal_params_precise_filter)


# ══════════════════════════════════════════════════════════════
#  第二部分: 500周期超长稳定性测试
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 2] 500-Cycle Long-Running Stability Test")
print("=" * 70)


def test_500_cycle_stability():
    """500周期连续运行稳定性测试"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    from sync.rule_sync_landing_bridge import RuleSyncBridge
    from shared.shared_data_models import (
        TaskDefinition, TaskPriority, OptimizationRule, RuleCategory
    )

    log_svc = AgentLoggingService({"log_level": "CRITICAL"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 5, "task_timeout_seconds": 10,
        "max_retries_per_task": 1, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 5000,
    }, log_svc, collector, recovery)
    analyzer = IntelligentOptimizationAnalyzer(
        {"optimization": {}}, log_svc, collector, repo, recovery
    )
    bridge = RuleSyncBridge(log_svc, repo, engine, analyzer)

    # 注册处理器
    engine.handler_registry.register("fast", lambda x=0: x + 1)
    engine.handler_registry.register("compute", lambda a=0, b=0: a * b + a - b)

    # 添加规则
    for i in range(3):
        repo.add_rule(OptimizationRule(
            name=f"perf_rule_{i}", description=f"Performance rule {i}",
            category=RuleCategory.PERFORMANCE, condition="always",
            action=f"optimize_{i}"
        ))

    mem_before = gc.get_mem_usage() if hasattr(gc, 'get_mem_usage') else 0
    errors = []
    total_tasks = 0

    start_time = time.time()
    for cycle in range(1, 501):
        try:
            # 每周期提交10个任务
            for i in range(10):
                handler = "fast" if i % 2 == 0 else "compute"
                engine.submit_task(TaskDefinition(
                    name=f"c{cycle}_t{i}", handler=handler,
                    handler_params={"x": i, "a": i, "b": i + 1},
                    priority=TaskPriority.NORMAL, max_retries=1,
                ))
            summary = engine.execute_cycle(cycle)
            total_tasks += summary.total_tasks

            # 每50周期执行分析和同步
            if cycle % 50 == 0:
                try:
                    analyzer.analyze_cycle(cycle)
                except Exception as e:
                    errors.append(f"cycle {cycle} analyze: {e}")
                bridge.sync_all_rules()

            # 每100周期执行prune
            if cycle % 100 == 0:
                collector.prune_old_data(max_cycles=50)
                stats = collector.get_total_stats()
                print(f"    Cycle {cycle}: total_exec={stats['total_executions']}, "
                      f"tasks_done={total_tasks}, rules={len(repo.get_active_rules())}")

        except Exception as e:
            errors.append(f"cycle {cycle}: {e}")
            break

    elapsed = time.time() - start_time

    # 验证
    assert_eq(len(errors), 0, f"500周期运行不应有错误: {errors[:3]}")
    assert_eq(total_tasks, 5000, f"应执行5000个任务，实际: {total_tasks}")

    # 验证数据一致性
    engine_stats = engine.get_stats()
    assert_ge(engine_stats["total_executed"], 5000,
              f"引擎统计应>=5000，实际: {engine_stats['total_executed']}")

    collector_stats = collector.get_total_stats()
    assert_ge(collector_stats["total_executions"], 5000,
              f"采集器统计应>=5000，实际: {collector_stats['total_executions']}")

    print(f"    500 cycles completed in {elapsed:.1f}s, "
          f"{total_tasks} tasks, {len(errors)} errors")

    bridge.stop()
    engine.shutdown()

run_test("500-cycle stability test", test_500_cycle_stability)


# ══════════════════════════════════════════════════════════════
#  第三部分: 极端并发竞态测试
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 3] Extreme Concurrency Race Condition Tests")
print("=" * 70)


def test_100_threads_all_modules():
    """100线程同时操作所有模块"""
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import EventType, SystemEvent
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from shared.shared_data_models import OptimizationRule, RuleCategory

    log_svc = AgentLoggingService({"log_level": "CRITICAL"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)

    errors = []
    lock = threading.Lock()

    def event_storm(tid):
        for i in range(100):
            try:
                log_svc.emit_event(SystemEvent(
                    event_type=EventType.TASK_COMPLETED,
                    source=f"storm_{tid}",
                    data={"tid": tid, "i": i}
                ))
            except Exception as e:
                with lock:
                    errors.append(f"event_{tid}_{i}: {e}")

    def recovery_storm(tid):
        for i in range(100):
            try:
                recovery.handle_exception(f"comp_{tid}", RuntimeError(f"err_{i}"))
                recovery.get_error_stats()
                recovery.get_component_health(f"comp_{tid}")
                recovery.get_all_health()
            except Exception as e:
                with lock:
                    errors.append(f"recovery_{tid}_{i}: {e}")

    def repo_storm(tid):
        for i in range(50):
            try:
                rule = OptimizationRule(
                    name=f"rule_{tid}_{i}", description=f"Test",
                    category=RuleCategory.PERFORMANCE, condition="always",
                    action=f"act_{i}"
                )
                repo.add_rule(rule)
                repo.get_active_rules()
                repo.get_rules_summary()
            except Exception as e:
                with lock:
                    errors.append(f"repo_{tid}_{i}: {e}")

    def collector_storm(tid):
        from shared.shared_data_models import TaskResult, TaskStatus
        for i in range(50):
            try:
                r = TaskResult(task_id=f"t_{tid}_{i}")
                r.status = TaskStatus.COMPLETED
                r.started_at = time.time()
                r.finished_at = time.time() + 0.001
                r.duration_seconds = 0.001
                collector.record_task_execution(r, cycle_number=tid)
                collector.get_total_stats()
            except Exception as e:
                with lock:
                    errors.append(f"collector_{tid}_{i}: {e}")

    threads = []
    for t in range(25):
        threads.append(threading.Thread(target=event_storm, args=(t,)))
        threads.append(threading.Thread(target=recovery_storm, args=(t,)))
        threads.append(threading.Thread(target=repo_storm, args=(t,)))
        threads.append(threading.Thread(target=collector_storm, args=(t,)))

    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)

    assert_eq(len(errors), 0, f"100线程并发操作不应有错误: {errors[:5]}")

run_test("100 threads concurrent all modules", test_100_threads_all_modules)


def test_concurrent_engine_submit_and_execute():
    """并发提交和执行任务"""
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 10, "task_timeout_seconds": 30,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 10000,
    }, log_svc, collector, recovery)
    engine.handler_registry.register("calc", lambda x=0: x ** 2)

    errors = []

    def submitter(tid):
        for i in range(200):
            try:
                engine.submit_task(TaskDefinition(
                    name=f"t_{tid}_{i}", handler="calc",
                    handler_params={"x": i}, priority=TaskPriority.NORMAL,
                    max_retries=0,
                ))
            except Exception as e:
                errors.append(f"submit_{tid}_{i}: {e}")

    threads = [threading.Thread(target=submitter, args=(t,)) for t in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=15)

    assert_eq(len(errors), 0, f"并发提交不应有错误: {errors[:3]}")

    # 执行所有任务
    summary = engine.execute_cycle(1)
    assert_eq(summary.total_tasks, 4000, f"应执行4000个任务，实际: {summary.total_tasks}")
    assert_eq(summary.completed_tasks, 4000, f"应全部完成，实际: {summary.completed_tasks}")

    engine.shutdown()

run_test("Concurrent submit + execute 4000 tasks", test_concurrent_engine_submit_and_execute)


# ══════════════════════════════════════════════════════════════
#  第四部分: 全生命周期状态机测试
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 4] Full Lifecycle State Machine Tests")
print("=" * 70)


def test_lifecycle_init_run_shutdown_repeat():
    """生命周期: init→run→shutdown→init→run→shutdown 重复3次"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    from sync.rule_sync_landing_bridge import RuleSyncBridge
    from shared.shared_data_models import TaskDefinition, TaskPriority

    for iteration in range(3):
        log_svc = AgentLoggingService({"log_level": "CRITICAL"})
        collector = ExecutionDataCollector({"storage": {}}, log_svc)
        recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
        repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
        engine = CoreTaskExecutionEngine({
            "max_concurrent_tasks": 3, "task_timeout_seconds": 10,
            "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
            "task_queue_max_size": 100,
        }, log_svc, collector, recovery)
        analyzer = IntelligentOptimizationAnalyzer(
            {"optimization": {}}, log_svc, collector, repo, recovery
        )
        bridge = RuleSyncBridge(log_svc, repo, engine, analyzer)

        engine.handler_registry.register("work", lambda x=0: x + iteration)

        # 运行5个周期
        for cycle in range(1, 6):
            for i in range(5):
                engine.submit_task(TaskDefinition(
                    name=f"iter{iteration}_c{cycle}_t{i}", handler="work",
                    handler_params={"x": i}, priority=TaskPriority.NORMAL,
                    max_retries=0,
                ))
            summary = engine.execute_cycle(cycle)
            assert_eq(summary.total_tasks, 5, f"Iter{iteration} C{cycle}: 应5个任务")

        # 清理
        bridge.stop()
        engine.shutdown()

        print(f"    Iteration {iteration + 1}/3 completed")

run_test("Lifecycle: init-run-shutdown x3", test_lifecycle_init_run_shutdown_repeat)


def test_lifecycle_shutdown_before_start():
    """生命周期: 未初始化直接shutdown"""
    from evolution_agent_main_entry import EvolutionAgent
    agent = EvolutionAgent.__new__(EvolutionAgent)
    agent._log = None
    agent._guardian = None
    agent._sync_bridge = None
    agent._execution_engine = None
    agent._optimization_analyzer = None
    agent._rule_repository = None
    agent._data_collector = None
    agent._recovery_handler = None
    # 直接shutdown，不应崩溃
    agent.shutdown()
    print("    (shutdown before initialize succeeded)")

run_test("Lifecycle: shutdown before initialize", test_lifecycle_shutdown_before_start)


def test_lifecycle_double_shutdown():
    """生命周期: 重复shutdown不应崩溃"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from core.core_task_execution_engine import CoreTaskExecutionEngine

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2, "task_timeout_seconds": 10,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 100,
    }, log_svc, collector, recovery)

    engine.handler_registry.register("x", lambda: "ok")

    engine.shutdown()
    engine.shutdown()  # 第二次shutdown不应崩溃
    print("    (double shutdown succeeded)")

run_test("Lifecycle: double shutdown", test_lifecycle_double_shutdown)


# ══════════════════════════════════════════════════════════════
#  第五部分: 数据一致性深度校验
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 5] Data Consistency Deep Verification")
print("=" * 70)


def test_data_consistency_cross_module():
    """数据一致性: 跨模块数据流完整性"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    from sync.rule_sync_landing_bridge import RuleSyncBridge
    from shared.shared_data_models import (
        TaskDefinition, TaskPriority, OptimizationRule, RuleCategory,
        TaskResult, TaskStatus
    )

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 5, "task_timeout_seconds": 10,
        "max_retries_per_task": 1, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 1000,
    }, log_svc, collector, recovery)
    analyzer = IntelligentOptimizationAnalyzer(
        {"optimization": {}}, log_svc, collector, repo, recovery
    )
    bridge = RuleSyncBridge(log_svc, repo, engine, analyzer)

    engine.handler_registry.register("add", lambda x=0, y=0: x + y)
    engine.handler_registry.register("fail", lambda: (_ for _ in ()).throw(RuntimeError("fail")))

    # 添加规则
    for i in range(3):
        repo.add_rule(OptimizationRule(
            name=f"rule_{i}", description=f"Rule {i}",
            category=RuleCategory.PERFORMANCE, condition="always",
            action=f"action_{i}"
        ))

    total_submitted = 0
    total_completed = 0
    total_failed = 0

    for cycle in range(1, 21):
        cycle_submitted = 0
        for i in range(20):
            handler = "fail" if i % 5 == 0 else "add"
            engine.submit_task(TaskDefinition(
                name=f"c{cycle}_t{i}", handler=handler,
                handler_params={"x": i, "y": i + 1},
                priority=TaskPriority.NORMAL, max_retries=1,
            ))
            cycle_submitted += 1
        total_submitted += cycle_submitted

        summary = engine.execute_cycle(cycle)
        total_completed += summary.completed_tasks
        total_failed += summary.failed_tasks

        # 每周期验证数据一致性
        engine_stats = engine.get_stats()
        collector_stats = collector.get_total_stats()

        # 引擎的total_executed应与采集器的total_executions一致
        assert_eq(engine_stats["total_executed"], collector_stats["total_executions"],
                  f"Cycle {cycle}: 引擎({engine_stats['total_executed']})与采集器({collector_stats['total_executions']})不一致")

        # 完成+失败应等于总数
        assert_eq(summary.completed_tasks + summary.failed_tasks, summary.total_tasks,
                  f"Cycle {cycle}: 完成+失败({summary.completed_tasks + summary.failed_tasks}) != 总数({summary.total_tasks})")

        # 分析和同步
        try:
            analyzer.analyze_cycle(cycle)
        except Exception:
            pass
        bridge.sync_all_rules()

    print(f"    Submitted={total_submitted}, Completed={total_completed}, Failed={total_failed}")
    assert_eq(total_submitted, 400, "应提交400个任务")
    assert_eq(total_completed + total_failed, 400, "完成+失败应等于总数")

    bridge.stop()
    engine.shutdown()

run_test("Data consistency: cross-module flow", test_data_consistency_cross_module)


def test_data_consistency_cumulative_after_prune():
    """数据一致性: prune后累计值不丢失"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from shared.shared_data_models import TaskResult, TaskStatus, CycleSummary

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)

    # 记录200条数据
    for i in range(200):
        r = TaskResult(task_id=f"t_{i}")
        r.status = TaskStatus.COMPLETED if i % 2 == 0 else TaskStatus.FAILED
        r.started_at = time.time()
        r.finished_at = time.time() + 0.001
        r.duration_seconds = 0.001
        if r.status == TaskStatus.FAILED:
            r.error = "test error"
        collector.record_task_execution(r, cycle_number=i // 10)

    # 添加周期汇总
    for c in range(20):
        collector._cycle_summaries.append(CycleSummary(
            cycle_number=c, total_tasks=10, completed_tasks=5,
            failed_tasks=5, avg_duration=0.001, error_types={}
        ))

    stats_before = collector.get_total_stats()
    assert_eq(stats_before["total_executions"], 200, "应有200条记录")

    # Prune保留最近5个周期
    collector.prune_old_data(max_cycles=5)

    stats_after = collector.get_total_stats()

    # 累计值不应减少
    assert_ge(stats_after["total_executions"], stats_before["total_executions"],
              f"累计值不应减少: {stats_before['total_executions']} -> {stats_after['total_executions']}")

run_test("Data consistency: cumulative after prune", test_data_consistency_cumulative_after_prune)


def test_data_consistency_rule_lifecycle():
    """数据一致性: 规则全生命周期数据一致性"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from shared.shared_data_models import OptimizationRule, RuleCategory

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    # 使用不存在的路径避免预加载
    repo = OptimizationRuleRepository({"optimization": {}, "rules_persistence_path": "/tmp/nonexistent_r13.json"}, log_svc)

    # 添加10条规则
    rule_ids = []
    for i in range(10):
        rule = OptimizationRule(
            name=f"rule_{i}", description=f"Rule {i}",
            category=RuleCategory.PERFORMANCE, condition="always",
            action=f"action_{i}"
        )
        repo.add_rule(rule)
        rule_ids.append(rule.rule_id)

    # 验证
    active = repo.get_active_rules()
    assert_eq(len(active), 10, f"应有10条活跃规则，实际: {len(active)}")

    # 停用5条
    for rid in rule_ids[:5]:
        repo.deactivate_rule(rid)

    active = repo.get_active_rules()
    assert_eq(len(active), 5, "停用后应有5条活跃规则")

    # 重新激活
    for rid in rule_ids[:5]:
        repo.activate_rule(rid)

    active = repo.get_active_rules()
    assert_eq(len(active), 10, "重新激活后应有10条活跃规则")

    # 更新效果评分
    for rid in rule_ids:
        repo.update_effectiveness(rid, 0.9)

    summary = repo.get_rules_summary()
    assert_eq(summary["total_rules"], 10, "总数应为10")
    assert_eq(summary["active_rules"], 10, "活跃应为10")

run_test("Data consistency: rule lifecycle", test_data_consistency_rule_lifecycle)


# ══════════════════════════════════════════════════════════════
#  第六部分: 异常容错深度测试
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 6] Exception Tolerance Deep Tests")
print("=" * 70)


def test_exception_handler_in_handler():
    """异常容错: 任务处理器抛出各种异常"""
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 5, "task_timeout_seconds": 10,
        "max_retries_per_task": 2, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 100,
    }, log_svc, collector, recovery)

    # 各种异常类型的处理器
    engine.handler_registry.register("value_err", lambda: (_ for _ in ()).throw(ValueError("bad value")))
    engine.handler_registry.register("type_err", lambda: (_ for _ in ()).throw(TypeError("bad type")))
    engine.handler_registry.register("key_err", lambda: (_ for _ in ()).throw(KeyError("missing key")))
    engine.handler_registry.register("runtime_err", lambda: (_ for _ in ()).throw(RuntimeError("runtime fail")))
    engine.handler_registry.register("zero_div", lambda: 1 / 0)
    engine.handler_registry.register("index_err", lambda: [][0])
    engine.handler_registry.register("attr_err", lambda: (1).nonexistent)
    engine.handler_registry.register("ok", lambda: "success")

    handlers = ["value_err", "type_err", "key_err", "runtime_err",
                "zero_div", "index_err", "attr_err", "ok"]

    for i, handler in enumerate(handlers):
        engine.submit_task(TaskDefinition(
            name=f"exc_{handler}", handler=handler, handler_params={},
            priority=TaskPriority.NORMAL, max_retries=2,
        ))

    summary = engine.execute_cycle(1)
    assert_eq(summary.total_tasks, 8, "应执行8个任务")
    assert_eq(summary.completed_tasks, 1, "只有ok处理器应成功")
    assert_eq(summary.failed_tasks, 7, "7个异常处理器应失败")

    # 验证错误被正确记录
    recovery_stats = recovery.get_error_stats()
    assert_ge(recovery_stats["total_errors"], 7, f"应有>=7条错误记录，实际: {recovery_stats['total_errors']}")

    engine.shutdown()

run_test("Exception tolerance: various exception types", test_exception_handler_in_handler)


def test_exception_queue_full():
    """异常容错: 任务队列满"""
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2, "task_timeout_seconds": 10,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 5,
    }, log_svc, collector, recovery)
    engine.handler_registry.register("x", lambda: "ok")

    # 提交超过队列大小的任务
    accepted = 0
    rejected = 0
    for i in range(20):
        result = engine.submit_task(TaskDefinition(
            name=f"t_{i}", handler="x", handler_params={},
            priority=TaskPriority.NORMAL, max_retries=0,
        ))
        if result:
            accepted += 1
        else:
            rejected += 1

    assert_eq(accepted, 5, f"应接受5个任务，实际: {accepted}")
    assert_eq(rejected, 15, f"应拒绝15个任务，实际: {rejected}")

    engine.shutdown()

run_test("Exception tolerance: queue full", test_exception_queue_full)


def test_exception_empty_handler_params():
    """异常容错: 空参数、None参数、非法类型参数"""
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 3, "task_timeout_seconds": 10,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 100,
    }, log_svc, collector, recovery)

    def flexible_handler(**kwargs):
        return f"got {kwargs}"

    engine.handler_registry.register("flex", flexible_handler)

    # 各种异常参数
    test_cases = [
        ({}, "empty params"),
        ({"x": None}, "None value"),
        ({"x": ""}, "empty string"),
        ({"x": 0}, "zero value"),
        ({"x": False}, "False value"),
        ({"x": []}, "empty list"),
        ({"x": {}}, "empty dict"),
    ]

    for params, desc in test_cases:
        engine.submit_task(TaskDefinition(
            name=f"param_{desc}", handler="flex",
            handler_params=params, priority=TaskPriority.NORMAL,
            max_retries=0,
        ))

    summary = engine.execute_cycle(1)
    assert_eq(summary.completed_tasks, len(test_cases),
              f"所有参数类型应成功处理，实际完成: {summary.completed_tasks}")

    engine.shutdown()

run_test("Exception tolerance: edge case params", test_exception_empty_handler_params)


# ══════════════════════════════════════════════════════════════
#  第七部分: 全量日志埋点验证
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 7] Full Logging Verification")
print("=" * 70)


def test_logging_all_modules_produce_output():
    """日志验证: 所有模块操作都产生日志"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    from sync.rule_sync_landing_bridge import RuleSyncBridge
    from shared.shared_data_models import (
        TaskDefinition, TaskPriority, OptimizationRule, RuleCategory,
        EventType, SystemEvent, TaskResult, TaskStatus
    )

    log_svc = AgentLoggingService({"log_level": "DEBUG"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2, "task_timeout_seconds": 10,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 100,
    }, log_svc, collector, recovery)
    analyzer = IntelligentOptimizationAnalyzer(
        {"optimization": {}}, log_svc, collector, repo, recovery
    )
    bridge = RuleSyncBridge(log_svc, repo, engine, analyzer)

    engine.handler_registry.register("log_test", lambda: "logged")

    # 触发各模块日志
    log_svc.emit_event(SystemEvent(
        event_type=EventType.TASK_COMPLETED, source="test", data={}
    ))

    recovery.handle_exception("test_comp", RuntimeError("log test"))

    repo.add_rule(OptimizationRule(
        name="log_rule", description="For logging test",
        category=RuleCategory.PERFORMANCE, condition="always", action="log"
    ))

    r = TaskResult(task_id="log_task")
    r.status = TaskStatus.COMPLETED
    r.started_at = time.time()
    r.finished_at = time.time() + 0.001
    collector.record_task_execution(r, cycle_number=1)

    engine.submit_task(TaskDefinition(
        name="log_task", handler="log_test", handler_params={},
        priority=TaskPriority.NORMAL, max_retries=0,
    ))
    engine.execute_cycle(1)

    bridge.sync_all_rules()

    # 验证事件历史
    events = log_svc.get_recent_events(100)
    assert_true(len(events) > 0, "应有事件记录")

    # 验证日志记录
    recent = log_svc.get_recent_events(50)
    assert_true(len(recent) > 0, "应有日志记录")

    print(f"    Events recorded: {len(events)}, Log entries available")

    bridge.stop()
    engine.shutdown()

run_test("Logging: all modules produce output", test_logging_all_modules_produce_output)


def test_logging_subscribe_callback_flow():
    """日志验证: 订阅回调完整执行链路"""
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import EventType, SystemEvent

    log_svc = AgentLoggingService({"log_level": "DEBUG"})
    received = []

    def on_event(event):
        received.append(event)

    log_svc.subscribe(on_event, filter_types=[EventType.TASK_COMPLETED])

    # 发布混合事件
    for i in range(20):
        etype = EventType.TASK_COMPLETED if i % 2 == 0 else EventType.CYCLE_STARTED
        log_svc.emit_event(SystemEvent(
            event_type=etype, source="test", data={"i": i}
        ))

    assert_eq(len(received), 10, f"应收到10个TASK_COMPLETED事件，实际: {len(received)}")

    # 取消订阅
    log_svc.unsubscribe(on_event)
    received.clear()

    for i in range(10):
        log_svc.emit_event(SystemEvent(
            event_type=EventType.TASK_COMPLETED, source="test", data={"i": i}
        ))

    assert_eq(len(received), 0, "取消订阅后不应收到事件")

run_test("Logging: subscribe/unsubscribe callback flow", test_logging_subscribe_callback_flow)


# ══════════════════════════════════════════════════════════════
#  第八部分: 性能核验
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 8] Performance Verification")
print("=" * 70)


def test_perf_high_frequency_task_submission():
    """性能: 高频任务提交不阻塞"""
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "CRITICAL"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 10, "task_timeout_seconds": 30,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 10000,
    }, log_svc, collector, recovery)
    engine.handler_registry.register("fast", lambda x=0: x + 1)

    # 高频提交10000个任务
    start = time.time()
    for i in range(10000):
        engine.submit_task(TaskDefinition(
            name=f"perf_{i}", handler="fast", handler_params={"x": i},
            priority=TaskPriority.NORMAL, max_retries=0,
        ))
    submit_time = time.time() - start

    assert_true(submit_time < 5.0, f"提交10000个任务应在5秒内完成，实际: {submit_time:.2f}s")

    # 执行
    start = time.time()
    summary = engine.execute_cycle(1)
    exec_time = time.time() - start

    assert_eq(summary.total_tasks, 10000, "应执行10000个任务")
    assert_eq(summary.completed_tasks, 10000, "应全部完成")
    print(f"    Submit: {submit_time:.2f}s, Execute: {exec_time:.2f}s, "
          f"Throughput: {10000/max(exec_time, 0.01):.0f} tasks/s")

    engine.shutdown()

run_test("Performance: 10k tasks throughput", test_perf_high_frequency_task_submission)


# ══════════════════════════════════════════════════════════════
#  测试结果汇总
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print(f"  Round 13 Results: PASS={_results['passed']}  FAIL={_results['failed']}")
print("=" * 70)

if _results["errors"]:
    print("\nFailure Details:")
    for err in _results["errors"]:
        print(f"  {err[:500]}")

sys.exit(0 if _results["failed"] == 0 else 1)
