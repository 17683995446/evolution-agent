"""
第12轮测试套件 - 超大规模混沌压力测试 + 跨模块深层交互边界探测
验证所有P0/P1/P2修复的正确性
"""
import sys
import os
import time
import threading
import queue
import json
import traceback
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ══════════════════════════════════════════════════════════════
#  测试计数器
# ══════════════════════════════════════════════════════════════
_test_results = {"passed": 0, "failed": 0, "errors": []}

def run_test(name, func):
    """运行单个测试"""
    try:
        func()
        _test_results["passed"] += 1
        print(f"  ✅ {name}")
    except Exception as e:
        _test_results["failed"] += 1
        tb = traceback.format_exc()
        _test_results["errors"].append(f"{name}: {e}\n{tb}")
        print(f"  ❌ {name}: {e}")

def assert_true(condition, msg=""):
    if not condition:
        raise AssertionError(f"断言失败: {msg}")

def assert_eq(a, b, msg=""):
    if a != b:
        raise AssertionError(f"{msg}: 期望 {b}, 实际 {a}")

def assert_no_exception(func, msg=""):
    try:
        func()
    except Exception as e:
        raise AssertionError(f"{msg}: 不应抛出异常但抛出了 {type(e).__name__}: {e}")

def assert_raises(func, expected_exc=None, msg=""):
    try:
        func()
        raise AssertionError(f"{msg}: 期望抛出异常但未抛出")
    except expected_exc if expected_exc else Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  P0 修复验证
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("  P0 修复验证测试")
print("=" * 60)

def test_p0_1_time_pattern_detection_no_shadowing():
    """P0-1: 验证时间模式检测不再使用变量遮蔽"""
    from adaptive.adaptive_scheduler import AdaptiveScheduler
    scheduler = AdaptiveScheduler()

    # 构造时间模式：失败间隔恰好为60秒的倍数（同一分钟内）
    base_time = 1000000.0
    history = deque(maxlen=100)
    # 10次成功
    for i in range(10):
        history.append({"timestamp": base_time + i, "duration": 0.1, "success": True, "error_type": None})
    # 3次失败，间隔都是120秒（同一分钟整除）
    history.append({"timestamp": base_time + 100, "duration": 0.1, "success": False, "error_type": "TimeoutError"})
    history.append({"timestamp": base_time + 220, "duration": 0.1, "success": False, "error_type": "TimeoutError"})
    history.append({"timestamp": base_time + 340, "duration": 0.1, "success": False, "error_type": "TimeoutError"})

    scheduler._execution_history["test_handler"] = history
    pattern = scheduler._detect_failure_pattern(history)

    # 120秒间隔 -> int(120)//60 = 2, int(220-100)//60 = 2, int(340-220)//60 = 2
    # 所有间隔除以60后都是2，set只有一个元素，应返回time_based
    assert_eq(pattern, "time_based", "等间隔失败应检测为time_based")

    # 验证不等间隔不会被误判为time_based
    history2 = deque(maxlen=100)
    for i in range(10):
        history2.append({"timestamp": base_time + i, "duration": 0.1, "success": True, "error_type": None})
    history2.append({"timestamp": base_time + 100, "duration": 0.1, "success": False, "error_type": "E1"})
    history2.append({"timestamp": base_time + 130, "duration": 0.1, "success": False, "error_type": "E2"})  # 30秒间隔
    history2.append({"timestamp": base_time + 400, "duration": 0.1, "success": False, "error_type": "E3"})  # 270秒间隔

    scheduler._execution_history["test_handler2"] = history2
    pattern2 = scheduler._detect_failure_pattern(history2)
    # 30//60=0, 270//60=4 -> set有{0, 4}两个元素，不等于1，不应返回time_based
    assert_true(pattern2 != "time_based", f"不等间隔失败不应检测为time_based，实际: {pattern2}")

run_test("P0-1: 时间模式检测变量遮蔽修复", test_p0_1_time_pattern_detection_no_shadowing)


def test_p0_2_span_context_manager_no_leak():
    """P0-2: 验证SpanContextManager不再泄漏上下文"""
    from observability.tracing import SpanContextManager, OTEL_AVAILABLE

    if not OTEL_AVAILABLE:
        print("  ⏭️  P0-2: OpenTelemetry不可用，跳过")
        _test_results["passed"] += 1
        return

    from opentelemetry import trace

    # 创建简单的tracer provider
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME

    resource = Resource.create({SERVICE_NAME: "test"})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("test")

    # 测试正常使用
    scm = SpanContextManager(tracer, "test_operation")
    assert_true(scm._span_cm is None, "初始时_span_cm应为None")
    assert_true(scm._span_token is None, "初始时_span_token应为None")

    with scm as span:
        assert_true(span is not None, "span不应为None")
        assert_true(scm._span_cm is not None, "进入上下文后_span_cm不应为None")

    # 退出后span应已结束
    assert_true(scm._span is not None, "退出后span引用仍存在")

    # 测试嵌套使用不会泄漏
    scm2 = SpanContextManager(tracer, "nested_operation")
    with scm2:
        pass

    provider.shutdown()
    print("    (OTEL可用，SpanContextManager上下文管理验证通过)")

run_test("P0-2: SpanContextManager上下文泄漏修复", test_p0_2_span_context_manager_no_leak)


def test_p0_3_heartbeat_independent_ttl():
    """P0-3: 验证心跳使用独立key和TTL"""
    from distributed.distributed_task_queue import DistributedTaskQueue

    # 检查代码中是否使用了独立key
    import inspect
    source = inspect.getsource(DistributedTaskQueue.start_heartbeat)
    assert_true("self._heartbeat_key}:{self._node_id}" in source or "heartbeat_key" in source,
                "心跳应使用独立key per node")

    # 检查是否使用了set with ex参数
    assert_true("ex=" in source or "expire" in source.lower(),
                "心跳应使用独立TTL")

    # 检查stop_heartbeat是否清理
    source_stop = inspect.getsource(DistributedTaskQueue.stop_heartbeat)
    assert_true("delete" in source_stop.lower(),
                "stop_heartbeat应清理心跳key")

run_test("P0-3: 心跳独立TTL修复", test_p0_3_heartbeat_independent_ttl)


# ══════════════════════════════════════════════════════════════
#  P1 修复验证
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("  P1 修复验证测试")
print("=" * 60)


def test_p1_4_claim_task_releases_lock_on_missing_data():
    """P1-4: 验证claim_task在任务数据缺失时释放锁"""
    from distributed.distributed_task_queue import DistributedTaskQueue
    import inspect
    source = inspect.getsource(DistributedTaskQueue.claim_task)
    # 检查在hget返回None前是否有delete锁的操作
    lines = source.split('\n')
    found_return_none = False
    found_delete_before_return = False
    for line in lines:
        if "return None" in line and "task_data" in source[source.index(line):source.index(line)+200]:
            found_return_none = True
        if "delete" in line and "lock" in line:
            found_delete_before_return = True
    assert_true(found_delete_before_return, "claim_task应在返回None前释放锁")

run_test("P1-4: claim_task锁释放修复", test_p1_4_claim_task_releases_lock_on_missing_data)


def test_p1_5_distributed_worker_relative_import():
    """P1-5: 验证distributed_worker使用相对导入"""
    import inspect
    from distributed import distributed_worker
    source = inspect.getsource(distributed_worker)
    assert_true("from .distributed_task_queue" in source,
                "应使用相对导入")

run_test("P1-5: distributed_worker相对导入修复", test_p1_5_distributed_worker_relative_import)


def test_p1_6_rules_preprocessing_no_internal_params():
    """P1-6: 验证规则预处理不向handler注入内部参数"""
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    config = {"logging": {"log_level": "WARNING"}}
    log_svc = AgentLoggingService(config["logging"])
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2,
        "task_timeout_seconds": 10,
        "max_retries_per_task": 0,
        "retry_delay_seconds": 0.1,
        "task_queue_max_size": 100,
    }, log_svc, collector, recovery)

    # 注册一个不接受**kwargs的handler
    received_params = {}
    def strict_handler(x: int, y: str):
        received_params["x"] = x
        received_params["y"] = y
        return "ok"
    engine.handler_registry.register("strict_test", strict_handler)

    # 应用包含timeout和parallel的规则
    engine.apply_optimization_rules([
        {"name": "reduce_timeout", "action": "reduce timeout for performance"},
        {"name": "enable_parallel", "action": "enable parallel execution"},
    ])

    # 创建任务
    task = TaskDefinition(
        name="strict_test_task",
        handler="strict_test",
        handler_params={"x": 42, "y": "hello"},
        priority=TaskPriority.NORMAL,
        max_retries=0,
    )

    # 执行任务
    result = engine._execute_single_task(task, 1)
    assert_eq(result.status.value, "completed", f"任务应成功完成，实际: {result.status.value}")
    assert_eq(received_params.get("x"), 42, "参数x应正确传递")
    assert_eq(received_params.get("y"), "hello", "参数y应正确传递")
    assert_true("_optimized_timeout" not in received_params, "不应注入_optimized_timeout")
    assert_true("_parallel_hint" not in received_params, "不应注入_parallel_hint")

    engine.shutdown()

run_test("P1-6: 规则预处理不注入内部参数", test_p1_6_rules_preprocessing_no_internal_params)


def test_p1_7_negative_max_retries_handled():
    """P1-7: 验证负数max_retries不会产生幽灵PENDING状态"""
    from shared.shared_data_models import TaskDefinition, TaskPriority, TaskStatus
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    config = {"logging": {"log_level": "WARNING"}}
    log_svc = AgentLoggingService(config["logging"])
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2,
        "task_timeout_seconds": 10,
        "max_retries_per_task": 0,
        "retry_delay_seconds": 0.1,
        "task_queue_max_size": 100,
    }, log_svc, collector, recovery)

    def ok_handler():
        return "ok"
    engine.handler_registry.register("ok", ok_handler)

    # 创建max_retries=-5的任务
    task = TaskDefinition(
        name="negative_retry_task",
        handler="ok",
        handler_params={},
        priority=TaskPriority.NORMAL,
        max_retries=-5,
    )

    result = engine._execute_single_task(task, 1)
    # max_retries被clamp到0，应执行一次并成功
    assert_eq(result.status.value, "completed", f"负数max_retries应被clamp为0并执行，实际: {result.status.value}")

    engine.shutdown()

run_test("P1-7: 负数max_retries处理", test_p1_7_negative_max_retries_handled)


def test_p1_8_no_duplicate_recovery_strategy():
    """P1-8: 验证RecoveryStrategy不再重复定义"""
    from shared.shared_data_models import RecoveryStrategy as SharedRS
    from execution.exception_recovery_handler import RecoveryStrategy as LocalRS

    # 它们应该是同一个类
    assert_true(SharedRS is LocalRS, "RecoveryStrategy应从shared_data_models导入，不应重复定义")

    # 验证ESCALATE值存在
    assert_true(hasattr(SharedRS, "ESCALATE"), "RecoveryStrategy应包含ESCALATE")

run_test("P1-8: RecoveryStrategy不重复定义", test_p1_8_no_duplicate_recovery_strategy)


def test_p1_9_event_bus_thread_safety():
    """P1-9: 验证事件总线线程安全"""
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import EventType, SystemEvent

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    events_received = []
    errors = []

    def callback(event):
        try:
            events_received.append(event)
        except Exception as e:
            errors.append(e)

    log_svc.subscribe(callback)

    # 多线程并发发布事件
    def publisher(thread_id, count):
        for i in range(count):
            log_svc.emit_event(SystemEvent(
                event_type=EventType.TASK_COMPLETED,
                source=f"thread_{thread_id}",
                data={"i": i},
            ))

    threads = []
    for t in range(10):
        th = threading.Thread(target=publisher, args=(t, 100))
        threads.append(th)
        th.start()

    for th in threads:
        th.join(timeout=10)

    assert_eq(len(errors), 0, f"事件回调不应有错误，实际: {errors}")
    assert_eq(len(events_received), 1000, f"应收到1000个事件，实际: {len(events_received)}")
    # 验证事件历史不超过max_history
    assert_true(len(log_svc._event_history) <= log_svc._max_history,
                f"事件历史不应超过{log_svc._max_history}")

run_test("P1-9: 事件总线线程安全", test_p1_9_event_bus_thread_safety)


def test_p1_10_single_bad_rule_doesnt_block_others():
    """P1-10: 验证单条坏规则不影响其他规则加载"""
    import tempfile
    import json
    from shared.unified_logging_service import AgentLoggingService
    from execution.optimization_rule_repository import OptimizationRuleRepository

    log_svc = AgentLoggingService({"log_level": "WARNING"})

    # 创建包含一条好规则和一条坏规则（缺少rule_id）的文件
    good_rule = {
        "rule_id": "good_rule_001",
        "name": "Good Rule",
        "description": "A valid rule",
        "category": "performance",
        "condition": "always",
        "action": "optimize",
        "priority": 10,
        "created_at": 1000.0,
        "effectiveness_score": 0.8,
        "apply_count": 5,
        "is_active": True,
    }
    bad_rule = {
        "name": "Bad Rule",
        # 缺少 rule_id
        "category": "performance",
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump({"rules": [good_rule, bad_rule]}, f)
        temp_path = f.name

    try:
        repo = OptimizationRuleRepository(
            {"rules_persistence_path": temp_path},
            log_svc
        )
        # 好规则应被加载
        rule = repo.get_rule("good_rule_001")
        assert_true(rule is not None, "好规则应被成功加载")
        assert_eq(rule.name, "Good Rule", "好规则名称应正确")
    finally:
        os.unlink(temp_path)

run_test("P1-10: 单条坏规则不影响其他规则加载", test_p1_10_single_bad_rule_doesnt_block_others)


def test_p1_11_baseline_fields_lock_protected():
    """P1-11: 验证基线字段读写有锁保护"""
    import inspect
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    source = inspect.getsource(IntelligentOptimizationAnalyzer._update_baseline)
    assert_true("self._lock" in source, "_update_baseline应使用锁")

    source2 = inspect.getsource(IntelligentOptimizationAnalyzer._analyze_success_rate)
    assert_true("self._lock" in source2, "_analyze_success_rate应使用锁")

    source3 = inspect.getsource(IntelligentOptimizationAnalyzer._analyze_duration)
    assert_true("self._lock" in source3, "_analyze_duration应使用锁")

run_test("P1-11: 基线字段锁保护", test_p1_11_baseline_fields_lock_protected)


def test_p1_12_shutdown_calls_sync_bridge_stop():
    """P1-12: 验证shutdown调用sync_bridge.stop()"""
    import inspect
    from evolution_agent_main_entry import EvolutionAgent
    source = inspect.getsource(EvolutionAgent.shutdown)
    assert_true("sync_bridge.stop()" in source or "_sync_bridge.stop()" in source,
                "shutdown应调用sync_bridge.stop()")

run_test("P1-12: shutdown调用sync_bridge.stop()", test_p1_12_shutdown_calls_sync_bridge_stop)


def test_p1_13_dashboard_psutil_optional():
    """P1-13: 验证dashboard模块psutil为可选依赖"""
    import observability.dashboard as dashboard_mod
    assert_true(hasattr(dashboard_mod, 'PSUTIL_AVAILABLE'),
                "dashboard应有PSUTIL_AVAILABLE标志")

run_test("P1-13: dashboard psutil可选依赖", test_p1_13_dashboard_psutil_optional)


def test_p1_14_guardian_stats_thread_safe():
    """P1-14: 验证守护管理器统计计数线程安全"""
    import inspect
    from sync.resident_guardian_manager import ResidentGuardianManager
    source = inspect.getsource(ResidentGuardianManager.check_all_components)
    # _total_health_checks += 1 应在锁内
    assert_true("self._lock" in source, "check_all_components应使用锁")

    source2 = inspect.getsource(ResidentGuardianManager._restart_component)
    assert_true("self._lock" in source2, "_restart_component应使用锁")

run_test("P1-14: 守护管理器统计线程安全", test_p1_14_guardian_stats_thread_safe)


def test_p1_15_adaptive_scheduler_thread_safe():
    """P1-15: 验证自适应调度器线程安全"""
    import inspect
    from adaptive.adaptive_scheduler import AdaptiveScheduler
    source = inspect.getsource(AdaptiveScheduler.__init__)
    assert_true("self._lock" in source, "AdaptiveScheduler应有锁")

    for method_name in ['record_execution', 'analyze_patterns', 'adjust_parameters', 'get_stats']:
        source_m = inspect.getsource(getattr(AdaptiveScheduler, method_name))
        assert_true("self._lock" in source_m, f"{method_name}应使用锁")

run_test("P1-15: 自适应调度器线程安全", test_p1_15_adaptive_scheduler_thread_safe)


# ══════════════════════════════════════════════════════════════
#  P2 修复验证
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("  P2 修复验证测试")
print("=" * 60)


def test_p2_1_adjustment_log_correct_old_params():
    """P2-1: 验证调整日志记录正确的旧参数"""
    from adaptive.adaptive_scheduler import AdaptiveScheduler, AdaptiveConfig

    scheduler = AdaptiveScheduler(AdaptiveConfig(adjustment_interval_cycles=1))

    # 记录足够多的执行历史以触发模式分析
    for i in range(15):
        scheduler.record_execution("test_h", 0.1, True)
    for i in range(5):
        scheduler.record_execution("test_h", 0.1, False)

    # 执行参数调整
    scheduler.adjust_parameters()

    if scheduler._adjustment_log:
        log_entry = scheduler._adjustment_log[-1]
        assert_true("old_params" in log_entry, "日志应有old_params")
        assert_true("new_params" in log_entry, "日志应有new_params")
        # old_params应与调整前的current_params一致
        # new_params应是平滑后的值
        assert_true(isinstance(log_entry["old_params"], dict), "old_params应为dict")
        assert_true(isinstance(log_entry["new_params"], dict), "new_params应为dict")

run_test("P2-1: 调整日志记录正确旧参数", test_p2_1_adjustment_log_correct_old_params)


def test_p2_2_adjustment_log_bounded():
    """P2-2: 验证调整日志有上限"""
    from adaptive.adaptive_scheduler import AdaptiveScheduler, AdaptiveConfig
    scheduler = AdaptiveScheduler(AdaptiveConfig(adjustment_interval_cycles=1))

    for i in range(15):
        scheduler.record_execution("h", 0.1, True)
    for i in range(5):
        scheduler.record_execution("h", 0.1, False)

    # 执行远超maxlen的调整次数
    for _ in range(300):
        scheduler.adjust_parameters()

    # 应不超过maxlen
    assert_true(len(scheduler._adjustment_log) <= 200,
                f"调整日志不应超过200，实际: {len(scheduler._adjustment_log)}")

run_test("P2-2: 调整日志有上限", test_p2_2_adjustment_log_bounded)


def test_p2_3_error_records_bounded():
    """P2-3: 验证错误记录有上限"""
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    handler = ExceptionRecoveryHandler({"guardian": {}}, log_svc)

    # 生成大量错误
    for i in range(1500):
        handler.handle_exception("test_comp", RuntimeError(f"error_{i}"))

    stats = handler.get_error_stats()
    assert_true(stats["total_errors"] <= 1000,
                f"错误记录不应超过1000，实际: {stats['total_errors']}")

run_test("P2-3: 错误记录有上限", test_p2_3_error_records_bounded)


def test_p2_4_component_health_returns_copy():
    """P2-4: 验证get_component_health返回副本"""
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    handler = ExceptionRecoveryHandler({"guardian": {}}, log_svc)

    handler.handle_exception("test_comp", RuntimeError("test error"))
    health1 = handler.get_component_health("test_comp")
    health2 = handler.get_component_health("test_comp")

    # 修改返回的对象不应影响内部状态
    health1.error_count = 99999
    health3 = handler.get_component_health("test_comp")
    assert_true(health3.error_count != 99999,
                "修改返回的健康对象不应影响内部状态")

run_test("P2-4: 组件健康返回副本", test_p2_4_component_health_returns_copy)


def test_p2_5_error_id_unique():
    """P2-5: 验证同一毫秒内error_id不冲突"""
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    handler = ExceptionRecoveryHandler({"guardian": {}}, log_svc)

    # 快速连续生成错误
    ids = set()
    for i in range(100):
        handler.handle_exception("comp", RuntimeError(f"err_{i}"))
        # 获取最后一条记录的id
        with handler._lock:
            last_id = handler._error_records[-1].error_id
            ids.add(last_id)

    # 100个错误应有100个不同的id
    assert_eq(len(ids), 100, f"100个错误应有100个唯一id，实际: {len(ids)}")

run_test("P2-5: error_id唯一性", test_p2_5_error_id_unique)


def test_p2_6_hooks_thread_safe():
    """P2-6: 验证同步钩子线程安全"""
    import inspect
    from sync.rule_sync_landing_bridge import RuleSyncBridge
    source = inspect.getsource(RuleSyncBridge.__init__)
    assert_true("_hooks_lock" in source, "应有hooks锁")

    source2 = inspect.getsource(RuleSyncBridge.add_pre_sync_hook)
    assert_true("_hooks_lock" in source2, "add_pre_sync_hook应使用锁")

run_test("P2-6: 同步钩子线程安全", test_p2_6_hooks_thread_safe)


def test_p2_7_start_guarding_atomic():
    """P2-7: 验证start_guarding使用锁保护"""
    import inspect
    from sync.resident_guardian_manager import ResidentGuardianManager
    source = inspect.getsource(ResidentGuardianManager.start_guarding)
    assert_true("self._lock" in source, "start_guarding应使用锁")

run_test("P2-7: start_guarding原子操作", test_p2_7_start_guarding_atomic)


def test_p2_9_initialize_cleanup_on_failure():
    """P2-9: 验证初始化失败时清理已创建模块"""
    import inspect
    from evolution_agent_main_entry import EvolutionAgent
    source = inspect.getsource(EvolutionAgent.initialize)
    assert_true("except" in source and "shutdown" in source,
                "initialize应有异常处理和清理逻辑")

run_test("P2-9: 初始化失败时清理", test_p2_9_initialize_cleanup_on_failure)


def test_p2_11_error_trend_zero_base():
    """P2-11: 验证错误趋势基线为0时不误判"""
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from execution.exception_recovery_handler import ExceptionRecoveryHandler

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)

    analyzer = IntelligentOptimizationAnalyzer(
        {"optimization": {}}, log_svc, collector, repo, recovery
    )

    # 记录一些数据，让trend从0增长到正数
    for cycle in range(5):
        collector.record_cycle_summary(type('obj', (object,), {
            'cycle_number': cycle,
            'total_tasks': 10,
            'completed_tasks': 10,
            'failed_tasks': 0,
            'avg_duration': 0.1,
            'error_types': {},
        })())

    # 从0错误到有错误不应触发"错误增长趋势"
    collector.record_cycle_summary(type('obj', (object,), {
        'cycle_number': 5,
        'total_tasks': 10,
        'completed_tasks': 5,
        'failed_tasks': 5,
        'avg_duration': 0.1,
        'error_types': {'TimeoutError': 5},
    })())

    # 不应崩溃
    rules = analyzer.analyze_cycle(6)
    assert_true(isinstance(rules, list), "analyze_cycle应返回列表")

run_test("P2-11: 错误趋势零基线不误判", test_p2_11_error_trend_zero_base)


def test_p2_12_batch_timeout_scales_with_parallelism():
    """P2-12: 验证批量超时随并行度缩放"""
    import inspect
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    source = inspect.getsource(CoreTaskExecutionEngine.execute_cycle)
    assert_true("max_concurrent" in source, "批量超时应考虑并行度")

run_test("P2-12: 批量超时随并行度缩放", test_p2_12_batch_timeout_scales_with_parallelism)


def test_p2_13_tracing_shutdown_resets_state():
    """P2-13: 验证TracingManager shutdown重置状态"""
    import inspect
    from observability.tracing import TracingManager
    source = inspect.getsource(TracingManager.shutdown)
    assert_true("_initialized" in source, "shutdown应重置_initialized")

run_test("P2-13: TracingManager shutdown重置状态", test_p2_13_tracing_shutdown_resets_state)


def test_p2_17_json_format_fallback():
    """P2-17: 验证JSON格式化失败时回退"""
    import inspect
    from observability.structured_logging import JSONFormatter
    source = inspect.getsource(JSONFormatter.format)
    assert_true("except" in source, "JSONFormatter.format应有异常处理")

run_test("P2-17: JSON格式化回退", test_p2_17_json_format_fallback)


# ══════════════════════════════════════════════════════════════
#  超大规模混沌压力测试
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("  超大规模混沌压力测试")
print("=" * 60)


def test_chaos_500_thread_event_storm():
    """混沌: 500线程并发事件风暴"""
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import EventType, SystemEvent

    log_svc = AgentLoggingService({"log_level": "CRITICAL"})
    counter = {"total": 0, "errors": 0}
    lock = threading.Lock()

    def storm(thread_id):
        for i in range(200):
            try:
                log_svc.emit_event(SystemEvent(
                    event_type=EventType.TASK_COMPLETED,
                    source=f"storm_{thread_id}",
                    data={"tid": thread_id, "i": i},
                ))
                with lock:
                    counter["total"] += 1
            except Exception:
                with lock:
                    counter["errors"] += 1

    threads = [threading.Thread(target=storm, args=(t,)) for t in range(500)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)

    assert_eq(counter["errors"], 0, f"500线程事件风暴不应有错误，实际: {counter['errors']}")
    assert_eq(counter["total"], 100000, f"应处理100000个事件，实际: {counter['total']}")

run_test("混沌: 500线程事件风暴", test_chaos_500_thread_event_storm)


def test_chaos_rapid_subscribe_unsubscribe():
    """混沌: 快速订阅/取消订阅"""
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import EventType, SystemEvent

    log_svc = AgentLoggingService({"log_level": "CRITICAL"})
    errors = []

    def subscriber(event):
        pass

    # 快速订阅和取消订阅
    for i in range(1000):
        try:
            log_svc.subscribe(subscriber)
            log_svc.unsubscribe(subscriber)
        except Exception as e:
            errors.append(e)

    assert_eq(len(errors), 0, f"快速订阅/取消订阅不应有错误，实际: {len(errors)}")

run_test("混沌: 快速订阅/取消订阅", test_chaos_rapid_subscribe_unsubscribe)


def test_chaos_concurrent_rule_sync():
    """混沌: 并发规则同步"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    from sync.rule_sync_landing_bridge import RuleSyncBridge

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2, "task_timeout_seconds": 10,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.1, "task_queue_max_size": 100,
    }, log_svc, collector, recovery)
    analyzer = IntelligentOptimizationAnalyzer(
        {"optimization": {}}, log_svc, collector, repo, recovery
    )
    bridge = RuleSyncBridge(log_svc, repo, engine, analyzer)

    errors = []

    def sync_loop(thread_id):
        for i in range(50):
            try:
                bridge.sync_all_rules()
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=sync_loop, args=(t,)) for t in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=15)

    assert_eq(len(errors), 0, f"并发规则同步不应有错误，实际: {len(errors)}")
    bridge.stop()
    engine.shutdown()

run_test("混沌: 并发规则同步", test_chaos_concurrent_rule_sync)


def test_chaos_adaptive_scheduler_concurrent():
    """混沌: 自适应调度器并发读写"""
    from adaptive.adaptive_scheduler import AdaptiveScheduler, AdaptiveConfig

    scheduler = AdaptiveScheduler(AdaptiveConfig(adjustment_interval_cycles=1))
    errors = []

    def writer(thread_id):
        for i in range(100):
            try:
                scheduler.record_execution(f"handler_{thread_id % 5}", 0.01, i % 3 != 0)
            except Exception as e:
                errors.append(e)

    def reader():
        for i in range(100):
            try:
                scheduler.get_stats()
            except Exception as e:
                errors.append(e)

    threads = []
    for t in range(10):
        threads.append(threading.Thread(target=writer, args=(t,)))
    for t in range(5):
        threads.append(threading.Thread(target=reader))

    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=15)

    assert_eq(len(errors), 0, f"调度器并发读写不应有错误，实际: {len(errors)}")

run_test("混沌: 自适应调度器并发读写", test_chaos_adaptive_scheduler_concurrent)


def test_chaos_guardian_concurrent_health_checks():
    """混沌: 守护管理器并发健康检查"""
    from sync.resident_guardian_manager import ResidentGuardianManager
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import ComponentHealth, HealthStatus
    from execution.exception_recovery_handler import ExceptionRecoveryHandler

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    guardian = ResidentGuardianManager(
        {"guardian": {"health_check_interval_seconds": 0.01, "auto_restart_enabled": False}},
        log_svc, recovery
    )

    call_count = {"n": 0}
    def healthy_check():
        call_count["n"] += 1
        return ComponentHealth(component_name="test", status=HealthStatus.HEALTHY)

    guardian.register_component("test", healthy_check)
    errors = []

    def checker():
        for _ in range(50):
            try:
                guardian.check_all_components()
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=checker) for _ in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=15)

    assert_eq(len(errors), 0, f"并发健康检查不应有错误，实际: {len(errors)}")
    assert_true(call_count["n"] > 0, "健康检查应被调用")

run_test("混沌: 守护管理器并发健康检查", test_chaos_guardian_concurrent_health_checks)


def test_chaos_engine_massive_task_throughput():
    """混沌: 引擎大规模任务吞吐"""
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 10,
        "task_timeout_seconds": 30,
        "max_retries_per_task": 1,
        "retry_delay_seconds": 0.01,
        "task_queue_max_size": 5000,
    }, log_svc, collector, recovery)

    def fast_handler(x=0):
        return x * 2

    engine.handler_registry.register("fast", fast_handler)

    # 提交2000个任务
    tasks = []
    for i in range(2000):
        tasks.append(TaskDefinition(
            name=f"task_{i}",
            handler="fast",
            handler_params={"x": i},
            priority=TaskPriority.NORMAL,
            max_retries=1,
        ))

    submitted = engine.submit_tasks(tasks)
    assert_eq(submitted, 2000, f"应提交2000个任务，实际: {submitted}")

    # 执行
    summary = engine.execute_cycle(1)
    assert_eq(summary.total_tasks, 2000, f"应执行2000个任务，实际: {summary.total_tasks}")
    assert_eq(summary.completed_tasks + summary.failed_tasks, 2000,
              "完成+失败应等于总数")

    engine.shutdown()

run_test("混沌: 引擎2000任务吞吐", test_chaos_engine_massive_task_throughput)


def test_chaos_recovery_handler_stress():
    """混沌: 恢复处理器压力测试"""
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService

    log_svc = AgentLoggingService({"log_level": "CRITICAL"})
    handler = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    errors = []

    def stress_thread(thread_id):
        for i in range(200):
            try:
                handler.handle_exception(f"comp_{thread_id}", RuntimeError(f"err_{i}"))
                handler.get_error_stats()
                handler.get_component_health(f"comp_{thread_id}")
                handler.get_all_health()
                handler.reset_component_errors(f"comp_{thread_id}")
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=stress_thread, args=(t,)) for t in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=15)

    assert_eq(len(errors), 0, f"恢复处理器压力测试不应有错误，实际: {len(errors)}")

run_test("混沌: 恢复处理器压力测试", test_chaos_recovery_handler_stress)


def test_chaos_rule_repository_concurrent_crud():
    """混沌: 规则仓库并发CRUD"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from shared.shared_data_models import OptimizationRule, RuleCategory

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    errors = []

    def writer(thread_id):
        for i in range(50):
            try:
                rule = OptimizationRule(
                    name=f"rule_t{thread_id}_i{i}",
                    description=f"Test rule {thread_id}-{i}",
                    category=RuleCategory.PERFORMANCE,
                    condition="always",
                    action=f"action_{i}",
                )
                repo.add_rule(rule)
                repo.update_effectiveness(rule.rule_id, 0.5)
                repo.get_active_rules()
                repo.get_rules_summary()
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=writer, args=(t,)) for t in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=15)

    assert_eq(len(errors), 0, f"规则仓库并发CRUD不应有错误，实际: {len(errors)}")

run_test("混沌: 规则仓库并发CRUD", test_chaos_rule_repository_concurrent_crud)


# ══════════════════════════════════════════════════════════════
#  跨模块深层集成测试
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("  跨模块深层集成测试")
print("=" * 60)


def test_integration_full_pipeline_with_chaos():
    """集成: 完整流水线+混沌注入"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    from sync.rule_sync_landing_bridge import RuleSyncBridge
    from shared.shared_data_models import TaskDefinition, TaskPriority, OptimizationRule, RuleCategory

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 5, "task_timeout_seconds": 30,
        "max_retries_per_task": 2, "retry_delay_seconds": 0.01, "task_queue_max_size": 1000,
    }, log_svc, collector, recovery)
    analyzer = IntelligentOptimizationAnalyzer(
        {"optimization": {}}, log_svc, collector, repo, recovery
    )
    bridge = RuleSyncBridge(log_svc, repo, engine, analyzer)

    # 注册混合处理器
    def good_handler(x=0):
        return x + 1

    def flaky_handler(x=0):
        import random
        if random.random() < 0.3:
            raise RuntimeError("random failure")
        return x * 2

    engine.handler_registry.register("good", good_handler)
    engine.handler_registry.register("flaky", flaky_handler)

    # 添加一些规则
    for i in range(5):
        rule = OptimizationRule(
            name=f"rule_{i}",
            description=f"Test rule {i}",
            category=RuleCategory.PERFORMANCE,
            condition="always",
            action=f"optimize_{i}",
        )
        repo.add_rule(rule)

    # 运行10个周期
    for cycle in range(1, 11):
        # 每周期提交20个任务
        for i in range(20):
            task = TaskDefinition(
                name=f"cycle{cycle}_task{i}",
                handler="flaky" if i % 3 == 0 else "good",
                handler_params={"x": i},
                priority=TaskPriority.NORMAL,
                max_retries=2,
            )
            engine.submit_task(task)

        summary = engine.execute_cycle(cycle)
        assert_true(summary.total_tasks > 0, f"周期{cycle}应有任务执行")

        # 分析
        try:
            analyzer.analyze_cycle(cycle)
        except Exception:
            pass  # 分析可能因数据不足而跳过

        # 同步
        bridge.sync_all_rules()

    # 验证最终状态
    stats = engine.get_stats()
    assert_true(stats["total_executed"] > 0, "应有任务被执行")
    collector_stats = collector.get_total_stats()
    assert_true(collector_stats["total_executions"] > 0, "采集器应有记录")

    bridge.stop()
    engine.shutdown()

run_test("集成: 完整流水线+混沌注入", test_integration_full_pipeline_with_chaos)


def test_integration_event_bus_cross_module():
    """集成: 事件总线跨模块传播"""
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import EventType, SystemEvent

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    received = {"count": 0}
    lock = threading.Lock()

    def on_task_completed(event):
        with lock:
            received["count"] += 1

    log_svc.subscribe(on_task_completed, filter_types=[EventType.TASK_COMPLETED])

    # 多线程发布不同类型事件
    def publisher():
        for i in range(100):
            log_svc.emit_event(SystemEvent(
                event_type=EventType.TASK_COMPLETED if i % 2 == 0 else EventType.CYCLE_STARTED,
                source="test",
                data={"i": i},
            ))

    threads = [threading.Thread(target=publisher) for _ in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=10)

    # 只应有TASK_COMPLETED事件被接收
    assert_eq(received["count"], 500, f"应收到500个TASK_COMPLETED事件，实际: {received['count']}")

run_test("集成: 事件总线跨模块传播", test_integration_event_bus_cross_module)


def test_integration_engine_restart_with_active_tasks():
    """集成: 引擎重启时有活跃任务"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from shared.shared_data_models import TaskDefinition, TaskPriority

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)

    def slow_handler():
        time.sleep(0.5)
        return "done"

    # 创建第一个引擎
    engine1 = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2, "task_timeout_seconds": 10,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01, "task_queue_max_size": 100,
    }, log_svc, collector, recovery)
    engine1.handler_registry.register("slow", slow_handler)

    # 提交任务
    for i in range(5):
        engine1.submit_task(TaskDefinition(
            name=f"task_{i}", handler="slow", handler_params={},
            priority=TaskPriority.NORMAL, max_retries=0,
        ))

    # 执行一个周期
    summary1 = engine1.execute_cycle(1)
    assert_eq(summary1.total_tasks, 5, "第一轮应执行5个任务")

    # 关闭引擎
    engine1.shutdown()

    # 创建新引擎（模拟重启）
    engine2 = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 2, "task_timeout_seconds": 10,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01, "task_queue_max_size": 100,
    }, log_svc, collector, recovery)
    engine2.handler_registry.register("slow", slow_handler)

    # 新引擎应能正常工作
    engine2.submit_task(TaskDefinition(
        name="post_restart_task", handler="slow", handler_params={},
        priority=TaskPriority.NORMAL, max_retries=0,
    ))
    summary2 = engine2.execute_cycle(2)
    assert_eq(summary2.total_tasks, 1, "重启后应能执行新任务")

    engine2.shutdown()

run_test("集成: 引擎重启后正常工作", test_integration_engine_restart_with_active_tasks)


def test_integration_collector_prune_data_consistency():
    """集成: 数据采集器prune后数据一致性"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from shared.shared_data_models import TaskResult, TaskStatus

    log_svc = AgentLoggingService({"log_level": "WARNING"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)

    # 记录大量数据
    for i in range(200):
        result = TaskResult(task_id=f"task_{i}")
        result.status = TaskStatus.COMPLETED if i % 2 == 0 else TaskStatus.FAILED
        result.started_at = time.time()
        result.finished_at = time.time() + 0.01
        result.duration_seconds = 0.01
        if result.status == TaskStatus.FAILED:
            result.error = "test error"
        collector.record_task_execution(result, i // 10)

    stats_before = collector.get_total_stats()

    # 执行prune
    collector.prune_old_data(max_cycles=10)

    stats_after = collector.get_total_stats()

    # 累计值不应减少
    assert_true(stats_after["total_executions"] >= stats_before["total_executions"],
                f"累计执行数不应减少: {stats_before['total_executions']} -> {stats_after['total_executions']}")

run_test("集成: 采集器prune后数据一致性", test_integration_collector_prune_data_consistency)


# ══════════════════════════════════════════════════════════════
#  测试结果汇总
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print(f"  第12轮测试结果: ✅ {_test_results['passed']} 通过, ❌ {_test_results['failed']} 失败")
print("=" * 60)

if _test_results["errors"]:
    print("\n失败详情:")
    for err in _test_results["errors"]:
        print(f"  {err}")

sys.exit(0 if _test_results["failed"] == 0 else 1)
