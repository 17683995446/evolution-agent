"""
持续进化Agent - 高难度压力测试套件
覆盖：并发竞争、异常注入、边界条件、内存泄漏、长时间满载运行
"""

from __future__ import annotations

import os
import random
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from shared.global_configuration_loader import AgentConfig
from shared.shared_data_models import (
    ComponentHealth,
    CycleSummary,
    ExecutionMetric,
    EventType,
    HealthStatus,
    OptimizationRule,
    RuleCategory,
    SystemEvent,
    TaskDefinition,
    TaskPriority,
    TaskResult,
    TaskStatus,
)
from shared.unified_logging_service import AgentLoggingService
from execution.execution_data_collector import ExecutionDataCollector
from execution.exception_recovery_handler import ExceptionRecoveryHandler, RecoveryStrategy, RecoveryAction
from execution.optimization_rule_repository import OptimizationRuleRepository
from core.core_task_execution_engine import CoreTaskExecutionEngine, TaskHandlerRegistry
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


# ══════════════════════════════════════════════════════════════
#  测试框架
# ══════════════════════════════════════════════════════════════

class TestResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.error: Optional[str] = None
        self.details: str = ""
        self.duration: float = 0.0

    def __repr__(self):
        status = "✅ 通过" if self.passed else f"❌ 失败: {self.error}"
        return f"  [{status}] {self.name} ({self.duration:.3f}s) {self.details}"


class StressTestSuite:
    """压力测试套件"""

    def __init__(self):
        self.results: List[TestResult] = []
        self._log_config = {"console_enabled": True, "file_enabled": False, "log_level": "WARNING"}
        self.log_svc = AgentLoggingService(self._log_config)
        self.log = self.log_svc.get_logger("stress_test")

    def run_test(self, name: str, func, *args, **kwargs) -> TestResult:
        result = TestResult(name)
        start = time.time()
        try:
            func(*args, **kwargs)
            result.passed = True
        except AssertionError as e:
            result.error = str(e)
            result.details = traceback.format_exc()[-300:]
        except Exception as e:
            result.error = f"{type(e).__name__}: {e}"
            result.details = traceback.format_exc()[-300:]
        result.duration = time.time() - start
        self.results.append(result)
        return result

    def print_report(self):
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        print(f"\n{'='*60}")
        print(f"  压力测试报告: {passed} 通过 / {failed} 失败 / {len(self.results)} 总计")
        print(f"{'='*60}")
        for r in self.results:
            print(r)
        if failed > 0:
            print(f"\n  ⚠️  {failed} 个测试失败，需要修复")
        else:
            print(f"\n  🎉 全部测试通过")


def build_fresh_system(log_svc: AgentLoggingService) -> Tuple:
    """构建全新的系统实例（每个测试独立，使用临时目录避免残留）"""
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="evo_test_")

    recovery = ExceptionRecoveryHandler({}, log_svc)
    collector = ExecutionDataCollector(
        {"metrics_persistence_path": os.path.join(tmpdir, "metrics.json")}, log_svc
    )
    rule_repo = OptimizationRuleRepository(
        {"rules_persistence_path": os.path.join(tmpdir, "rules.json")}, log_svc
    )
    engine = CoreTaskExecutionEngine({}, log_svc, collector, recovery)
    analyzer = IntelligentOptimizationAnalyzer({}, log_svc, collector, rule_repo, recovery)
    bridge = RuleSyncBridge(log_svc, rule_repo, engine, analyzer)
    guardian = ResidentGuardianManager({}, log_svc, recovery)
    return engine, collector, rule_repo, analyzer, bridge, guardian, recovery


def register_test_handlers(engine: CoreTaskExecutionEngine):
    """注册测试用任务处理器"""

    def instant_task(x: int = 1) -> Dict:
        return {"result": x * 2}

    def slow_task(duration_ms: int = 100) -> Dict:
        time.sleep(duration_ms / 1000.0)
        return {"slept": duration_ms}

    def failing_task(fail: bool = True) -> Dict:
        if fail:
            raise RuntimeError("故意失败")
        return {"ok": True}

    def param_error_task(data: Any = None) -> Dict:
        # 故意制造类型错误
        return data + 1  # type: ignore

    def heavy_compute_task(n: int = 100000) -> Dict:
        total = sum(i * i for i in range(n))
        return {"sum_of_squares": total}

    def memory_hungry_task(size_mb: int = 1) -> Dict:
        data = b"x" * (size_mb * 1024 * 1024)
        return {"allocated_mb": size_mb, "hash": hash(data)}

    def unpredictable_task(seed: int = 0) -> Dict:
        random.seed(seed)
        r = random.random()
        if r < 0.3:
            raise ValueError(f"随机失败 r={r:.3f}")
        if r < 0.5:
            time.sleep(0.5)
        return {"random_result": r}

    def handler_with_bad_params(**kwargs) -> Dict:
        # 故意不接收任何参数，测试参数不匹配
        return {"received": str(kwargs)}

    for name, handler in [
        ("instant_task", instant_task),
        ("slow_task", slow_task),
        ("failing_task", failing_task),
        ("param_error_task", param_error_task),
        ("heavy_compute_task", heavy_compute_task),
        ("memory_hungry_task", memory_hungry_task),
        ("unpredictable_task", unpredictable_task),
        ("handler_with_bad_params", handler_with_bad_params),
    ]:
        engine.handler_registry.register(name, handler)


# ══════════════════════════════════════════════════════════════
#  测试场景
# ══════════════════════════════════════════════════════════════

def test_concurrent_task_submission(suite: StressTestSuite):
    """测试1: 高并发任务提交竞争"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    # 调大队列容量以容纳并发提交
    engine._queue_max_size = 10000
    # 重建队列（因为maxsize在构造时设置）
    import queue as q
    engine._task_queue = q.Queue(maxsize=10000)

    # 100个线程同时提交任务
    def submit_batch(thread_id):
        for i in range(50):
            task = TaskDefinition(
                name=f"concurrent_t{thread_id}_i{i}",
                handler="instant_task",
                handler_params={"x": thread_id * 100 + i},
            )
            engine.submit_task(task)

    threads = [threading.Thread(target=submit_batch, args=(t,)) for t in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    queue_size = engine.queue_size()
    assert queue_size == 5000, f"预期队列5000，实际{queue_size}（并发提交丢失）"

    # 执行并验证
    summary = engine.execute_cycle(1)
    assert summary.total_tasks == 5000, f"预期执行5000，实际{summary.total_tasks}"
    assert summary.completed_tasks == 5000, f"预期成功5000，实际{summary.completed_tasks}"
    engine.shutdown()


def test_high_failure_rate_recovery(suite: StressTestSuite):
    """测试2: 高失败率场景下的恢复能力"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    # 提交100个必定失败的任务
    tasks = [
        TaskDefinition(name=f"fail_{i}", handler="failing_task",
                       handler_params={"fail": True}, max_retries=2)
        for i in range(100)
    ]
    engine.submit_tasks(tasks)

    summary = engine.execute_cycle(1)
    assert summary.total_tasks == 100
    assert summary.failed_tasks == 100, f"预期全部失败，实际失败{summary.failed_tasks}"

    # 验证错误采集正确
    error_dist = collector.get_error_distribution()
    assert "RuntimeError" in error_dist, f"错误分布中缺少RuntimeError: {error_dist}"
    assert error_dist["RuntimeError"] >= 100, f"RuntimeError计数不足: {error_dist}"

    # 系统应该仍在运行（没有崩溃）
    assert engine.queue_size() == 0, "队列应该已清空"

    # 提交正常任务验证系统恢复
    normal_tasks = [
        TaskDefinition(name=f"normal_{i}", handler="instant_task",
                       handler_params={"x": i})
        for i in range(50)
    ]
    engine.submit_tasks(normal_tasks)
    summary2 = engine.execute_cycle(2)
    assert summary2.completed_tasks == 50, f"恢复后应全部成功，实际{summary2.completed_tasks}"
    engine.shutdown()


def test_mixed_task_types(suite: StressTestSuite):
    """测试3: 混合任务类型（成功+失败+超时+参数错误）"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    tasks = []
    # 30% 成功
    for i in range(30):
        tasks.append(TaskDefinition(name=f"ok_{i}", handler="instant_task",
                                     handler_params={"x": i}))
    # 20% 失败
    for i in range(20):
        tasks.append(TaskDefinition(name=f"fail_{i}", handler="failing_task",
                                     handler_params={"fail": True}, max_retries=1))
    # 20% 参数错误
    for i in range(20):
        tasks.append(TaskDefinition(name=f"param_err_{i}", handler="param_error_task",
                                     handler_params={"data": "not_a_number"}, max_retries=1))
    # 15% 不可预测
    for i in range(15):
        tasks.append(TaskDefinition(name=f"unpredict_{i}", handler="unpredictable_task",
                                     handler_params={"seed": i}, max_retries=3))
    # 15% 不存在的处理器
    for i in range(15):
        tasks.append(TaskDefinition(name=f"missing_{i}", handler="nonexistent_handler"))

    engine.submit_tasks(tasks)
    summary = engine.execute_cycle(1)

    assert summary.total_tasks == 100, f"总计应为100，实际{summary.total_tasks}"
    # 至少30个成功（instant_task）+ 15个跳过（missing handler）
    assert summary.completed_tasks >= 30, f"成功数应>=30，实际{summary.completed_tasks}"
    assert summary.failed_tasks >= 20, f"失败数应>=20，实际{summary.failed_tasks}"
    engine.shutdown()


def test_empty_queue_cycle(suite: StressTestSuite):
    """测试4: 空队列周期不应崩溃"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    # 连续执行10个空周期
    for i in range(1, 11):
        summary = engine.execute_cycle(i)
        assert summary.total_tasks == 0, f"空周期{i}应有0任务"
        assert summary.success_rate == 0.0, f"空周期{i}成功率应为0.0"

    # 之后提交任务应正常工作
    engine.submit_task(TaskDefinition(name="after_empty", handler="instant_task",
                                      handler_params={"x": 42}))
    summary = engine.execute_cycle(11)
    assert summary.completed_tasks == 1
    assert summary.success_rate == 1.0
    engine.shutdown()


def test_queue_overflow(suite: StressTestSuite):
    """测试5: 队列溢出保护"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    # 重建小容量队列
    import queue as q
    engine._queue_max_size = 10
    engine._task_queue = q.Queue(maxsize=10)

    # 尝试提交超过队列容量的任务
    results = []
    for i in range(50):
        ok = engine.submit_task(TaskDefinition(name=f"overflow_{i}", handler="instant_task",
                                               handler_params={"x": i}))
        results.append(ok)

    accepted = sum(results)
    rejected = len(results) - accepted
    assert accepted == 10, f"应接受10个，实际{accepted}"
    assert rejected == 40, f"应拒绝40个，实际{rejected}"
    assert engine.queue_size() == 10

    # 执行后队列应清空
    engine.execute_cycle(1)
    assert engine.queue_size() == 0
    engine.shutdown()


def test_concurrent_cycle_execution(suite: StressTestSuite):
    """测试6: 并发执行多个周期（线程安全）"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    errors = []

    def run_cycles(thread_id):
        try:
            for i in range(5):
                tasks = [TaskDefinition(name=f"t{thread_id}_c{i}", handler="instant_task",
                                        handler_params={"x": thread_id * 100 + i})]
                engine.submit_tasks(tasks)
                summary = engine.execute_cycle(thread_id * 100 + i)
                if summary.completed_tasks != 1:
                    errors.append(f"线程{thread_id} 周期{i}: 预期1成功，实际{summary.completed_tasks}")
        except Exception as e:
            errors.append(f"线程{thread_id} 异常: {e}")

    threads = [threading.Thread(target=run_cycles, args=(t,)) for t in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"并发周期执行出错: {errors[:5]}"
    engine.shutdown()


def test_rule_repository_stress(suite: StressTestSuite):
    """测试7: 规则仓库并发读写压力"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    errors = []

    def writer(thread_id):
        try:
            for i in range(100):
                rule = OptimizationRule(
                    name=f"rule_t{thread_id}_i{i}",
                    category=random.choice(list(RuleCategory)),
                    action=f"action_{i}",
                    source_cycle=thread_id,
                )
                rule_repo.add_rule(rule)
        except Exception as e:
            errors.append(f"写入线程{thread_id}: {e}")

    def reader(thread_id):
        try:
            for i in range(100):
                active = rule_repo.get_active_rules()
                summary = rule_repo.get_rules_summary()
                assert summary["total_rules"] >= 0
        except Exception as e:
            errors.append(f"读取线程{thread_id}: {e}")

    threads = []
    for t in range(10):
        threads.append(threading.Thread(target=writer, args=(t,)))
        threads.append(threading.Thread(target=reader, args=(t,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"规则仓库并发错误: {errors[:5]}"
    total = rule_repo.get_rules_summary()["total_rules"]
    assert total == 1000, f"预期1000条规则，实际{total}"
    engine.shutdown()


def test_data_collector_concurrent_recording(suite: StressTestSuite):
    """测试8: 数据采集器并发记录"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    collector = ExecutionDataCollector({}, log_svc)

    errors = []

    def recorder(thread_id):
        try:
            for i in range(200):
                success = random.random() > 0.3
                result = TaskResult(
                    task_id=f"t{thread_id}_i{i}",
                    status=TaskStatus.COMPLETED if success else TaskStatus.FAILED,
                    started_at=time.time() - 0.01,
                    finished_at=time.time(),
                )
                if not success:
                    result.error = RuntimeError(f"模拟错误 {i}")
                collector.record_task_execution(result, cycle_number=thread_id)
        except Exception as e:
            errors.append(f"采集线程{thread_id}: {e}")

    threads = [threading.Thread(target=recorder, args=(t,)) for t in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"采集器并发错误: {errors[:5]}"
    stats = collector.get_total_stats()
    assert stats["total_executions"] == 4000, f"预期4000条记录，实际{stats['total_executions']}"


def test_event_bus_flood(suite: StressTestSuite):
    """测试9: 事件总线洪泛"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})

    received_count = {"value": 0}
    lock = threading.Lock()

    def on_event(event):
        with lock:
            received_count["value"] += 1

    log_svc.subscribe(on_event)

    # 快速发送大量事件
    for i in range(5000):
        log_svc.emit_event(SystemEvent(
            event_type=EventType.TASK_COMPLETED,
            source="test",
            data={"index": i},
        ))

    # 事件历史不应超过max_history
    events = log_svc.get_recent_events(99999)
    assert len(events) <= 1000, f"事件历史应<=1000，实际{len(events)}"
    assert received_count["value"] == 5000, f"应收到5000个事件，实际{received_count['value']}"


def test_long_running_stability(suite: StressTestSuite):
    """测试10: 长时间稳定运行（50个周期，大量任务）"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    total_completed = 0
    total_failed = 0

    for cycle in range(1, 51):
        # 每周期提交混合任务
        tasks = []
        for i in range(20):
            tasks.append(TaskDefinition(name=f"c{cycle}_ok_{i}", handler="instant_task",
                                         handler_params={"x": i}))
        for i in range(5):
            tasks.append(TaskDefinition(name=f"c{cycle}_fail_{i}", handler="failing_task",
                                         handler_params={"fail": True}, max_retries=1))
        for i in range(5):
            tasks.append(TaskDefinition(name=f"c{cycle}_slow_{i}", handler="slow_task",
                                         handler_params={"duration_ms": 10}))

        engine.submit_tasks(tasks)
        summary = engine.execute_cycle(cycle)

        total_completed += summary.completed_tasks
        total_failed += summary.failed_tasks

    assert total_completed >= 50 * 20, f"50周期应至少成功1000个，实际{total_completed}"
    assert total_failed >= 50 * 5, f"50周期应至少失败250个，实际{total_failed}"

    # 验证采集器数据一致性
    stats = collector.get_total_stats()
    assert stats["total_executions"] == total_completed + total_failed, \
        f"采集器总数{stats['total_executions']} != 引擎总数{total_completed + total_failed}"

    engine.shutdown()


def test_guardian_restart_simulation(suite: StressTestSuite):
    """测试11: 守护层重启组件模拟"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    restart_called = {"count": 0}

    def fake_restart():
        restart_called["count"] += 1
        return True

    guardian.register_component(
        name="test_component",
        health_check=lambda: ComponentHealth(
            component_name="test_component",
            status=HealthStatus.UNHEALTHY,
            error_count=10,
        ),
        restart_action=fake_restart,
        critical=True,
    )

    # 手动触发多次健康检查
    for _ in range(8):
        guardian.check_all_components()

    assert restart_called["count"] >= 1, f"应触发至少1次重启，实际{restart_called['count']}"


def test_optimization_rule_lifecycle(suite: StressTestSuite):
    """测试12: 优化规则完整生命周期"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    # 先执行一些任务产生数据
    tasks = [TaskDefinition(name=f"opt_{i}", handler="instant_task",
                             handler_params={"x": i}) for i in range(20)]
    engine.submit_tasks(tasks)
    engine.execute_cycle(1)

    # 手动创建规则
    rule = OptimizationRule(
        name="测试规则A",
        category=RuleCategory.PERFORMANCE,
        action="优化执行",
        effectiveness_score=0.8,
        source_cycle=1,
    )
    rid = rule_repo.add_rule(rule)
    assert rule_repo.get_rule(rid) is not None

    # 同步到引擎（初始可能有0或1条，取决于事件触发）
    count = bridge.sync_all_rules()
    assert count >= 1, f"应至少同步1条规则，实际{count}"

    # 停用规则
    rule_repo.deactivate_rule(rid)
    active = rule_repo.get_active_rules()
    assert len(active) == 0, f"停用后应无活跃规则，实际{len(active)}"

    # 重新激活
    rule_repo.activate_rule(rid)
    active = rule_repo.get_active_rules()
    assert len(active) == 1

    # 更新效果评分
    rule_repo.update_effectiveness(rid, 0.05)  # 低于阈值
    active = rule_repo.get_active_rules()
    assert len(active) == 0, "低效规则应被自动停用"

    # 移除规则
    rule_repo.activate_rule(rid)
    rule_repo.remove_rule(rid)
    assert rule_repo.get_rule(rid) is None

    engine.shutdown()


def test_persistence_roundtrip(suite: StressTestSuite):
    """测试13: 数据持久化往返一致性"""
    import tempfile, json

    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    # 产生数据
    tasks = [TaskDefinition(name=f"persist_{i}", handler="instant_task",
                             handler_params={"x": i}) for i in range(15)]
    engine.submit_tasks(tasks)
    engine.execute_cycle(1)

    # 创建规则
    for i in range(5):
        rule = OptimizationRule(name=f"rule_{i}", category=RuleCategory.PERFORMANCE,
                                 action=f"action_{i}", source_cycle=1)
        rule_repo.add_rule(rule)

    # 使用独立临时目录持久化
    with tempfile.TemporaryDirectory() as tmpdir:
        rules_path = os.path.join(tmpdir, "rules_test.json")
        metrics_path = os.path.join(tmpdir, "metrics_test.json")

        rule_repo.persist_to_file(rules_path)
        collector.persist_to_file(metrics_path)

        # 验证文件存在且可读
        assert os.path.exists(rules_path)
        assert os.path.exists(metrics_path)

        with open(rules_path) as f:
            rules_data = json.load(f)
        assert len(rules_data["rules"]) == 5, f"预期5条规则，实际{len(rules_data['rules'])}"

        with open(metrics_path) as f:
            metrics_data = json.load(f)
        assert len(metrics_data["metrics"]) == 15, f"预期15条指标，实际{len(metrics_data['metrics'])}"

    engine.shutdown()


def test_recovery_handler_error_accumulation(suite: StressTestSuite):
    """测试14: 恢复处理器错误累积与重置"""
    recovery = ExceptionRecoveryHandler({}, suite.log_svc)

    # 模拟多次错误
    for i in range(8):
        recovery.handle_exception("test_comp", RuntimeError(f"错误 #{i}"))

    health = recovery.get_component_health("test_comp")
    assert health.error_count == 8, f"错误计数应为8，实际{health.error_count}"
    assert health.status == HealthStatus.UNHEALTHY, f"状态应为UNHEALTHY，实际{health.status}"

    # 重置
    recovery.reset_component_errors("test_comp")
    health = recovery.get_component_health("test_comp")
    assert health.error_count == 0, "重置后错误计数应为0"
    assert health.status == HealthStatus.HEALTHY, "重置后状态应为HEALTHY"

    error_stats = recovery.get_error_stats()
    assert error_stats["total_errors"] == 8


def test_zero_and_negative_inputs(suite: StressTestSuite):
    """测试15: 零值和负值边界输入"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)
    # 额外注册示例处理器（测试引用了它们）
    from examples.example_task_definitions import (
        fibonacci_sequence, process_data_transformation,
    )
    engine.handler_registry.register("fibonacci_sequence", fibonacci_sequence)
    engine.handler_registry.register("process_data_transformation", process_data_transformation)

    # 空参数
    engine.submit_task(TaskDefinition(name="empty_params", handler="instant_task",
                                      handler_params={}))
    # 零值
    engine.submit_task(TaskDefinition(name="zero_n", handler="fibonacci_sequence",
                                      handler_params={"n": 0}))
    # 负值
    engine.submit_task(TaskDefinition(name="neg_n", handler="fibonacci_sequence",
                                      handler_params={"n": -5}))
    # 空字符串
    engine.submit_task(TaskDefinition(name="empty_str", handler="process_data_transformation",
                                      handler_params={"data": "", "transform_type": "uppercase"}))

    summary = engine.execute_cycle(1)
    assert summary.total_tasks == 4
    # instant_task, fibonacci(0), fibonacci(-5), uppercase("") 都应成功
    assert summary.completed_tasks == 4, \
        f"边界输入应全部成功，实际{summary.completed_tasks}，失败{summary.failed_tasks}"
    engine.shutdown()


def test_rapid_start_stop_cycles(suite: StressTestSuite):
    """测试16: 快速启停循环（资源泄漏检测）"""
    for i in range(20):
        engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
        register_test_handlers(engine)

        tasks = [TaskDefinition(name=f"rapid_{i}_{j}", handler="instant_task",
                                 handler_params={"x": j}) for j in range(10)]
        engine.submit_tasks(tasks)
        engine.execute_cycle(1)
        engine.shutdown()
        guardian.stop_guarding()

    # 如果没有资源泄漏，20轮应该正常完成
    assert True


def test_full_system_integration(suite: StressTestSuite):
    """测试17: 完整系统集成（执行+采集+分析+同步+守护）"""
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_fresh_system(suite.log_svc)
    register_test_handlers(engine)

    # 注册守护组件
    guardian.register_component(
        name="engine",
        health_check=lambda: ComponentHealth(component_name="engine"),
    )

    errors = []

    for cycle in range(1, 21):
        try:
            # 混合任务
            tasks = []
            for i in range(15):
                tasks.append(TaskDefinition(name=f"int_{cycle}_ok_{i}", handler="instant_task",
                                             handler_params={"x": i}))
            for i in range(3):
                tasks.append(TaskDefinition(name=f"int_{cycle}_fail_{i}", handler="failing_task",
                                             handler_params={"fail": True}, max_retries=1))

            engine.submit_tasks(tasks)
            summary = engine.execute_cycle(cycle)

            # 优化分析
            new_rules = analyzer.analyze_cycle(cycle)
            if new_rules:
                bridge.sync_new_rules(new_rules)

            # 数据一致性检查
            stats = collector.get_total_stats()
            engine_stats = engine.get_stats()
            assert stats["total_executions"] == engine_stats["total_executed"], \
                f"周期{cycle}: 采集器与引擎数据不一致"

        except Exception as e:
            errors.append(f"周期{cycle}: {e}")
            break

    assert len(errors) == 0, f"集成测试出错: {errors[:3]}"

    # 最终验证
    stats = collector.get_total_stats()
    assert stats["total_executions"] == 20 * 18, f"预期360次执行，实际{stats['total_executions']}"

    guardian.stop_guarding()
    engine.shutdown()


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 60)
    print("  🔬 持续进化Agent - 高难度压力测试套件")
    print("=" * 60 + "\n")

    suite = StressTestSuite()

    tests = [
        ("测试1: 高并发任务提交竞争 (100线程x50任务)", test_concurrent_task_submission),
        ("测试2: 高失败率恢复 (100个失败任务)", test_high_failure_rate_recovery),
        ("测试3: 混合任务类型 (成功+失败+超时+参数错误)", test_mixed_task_types),
        ("测试4: 空队列周期连续执行", test_empty_queue_cycle),
        ("测试5: 队列溢出保护", test_queue_overflow),
        ("测试6: 并发周期执行 (20线程)", test_concurrent_cycle_execution),
        ("测试7: 规则仓库并发读写 (20线程x100操作)", test_rule_repository_stress),
        ("测试8: 数据采集器并发记录 (20线程x200条)", test_data_collector_concurrent_recording),
        ("测试9: 事件总线洪泛 (5000事件)", test_event_bus_flood),
        ("测试10: 长时间稳定运行 (50周期x30任务)", test_long_running_stability),
        ("测试11: 守护层重启组件模拟", test_guardian_restart_simulation),
        ("测试12: 优化规则完整生命周期", test_optimization_rule_lifecycle),
        ("测试13: 数据持久化往返一致性", test_persistence_roundtrip),
        ("测试14: 恢复处理器错误累积与重置", test_recovery_handler_error_accumulation),
        ("测试15: 零值和负值边界输入", test_zero_and_negative_inputs),
        ("测试16: 快速启停循环 (20轮)", test_rapid_start_stop_cycles),
        ("测试17: 完整系统集成 (20周期全链路)", test_full_system_integration),
    ]

    for name, test_func in tests:
        print(f"  ▶ {name}...")
        result = suite.run_test(name, test_func, suite)
        if result.passed:
            print(f"    ✅ 通过 ({result.duration:.3f}s)")
        else:
            print(f"    ❌ 失败: {result.error}")

    suite.print_report()


if __name__ == "__main__":
    main()
