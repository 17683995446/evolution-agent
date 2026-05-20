"""
持续进化Agent - 第二轮深层架构测试
针对代码审阅发现的深层问题进行验证
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from shared.global_configuration_loader import AgentConfig
from shared.shared_data_models import (
    ComponentHealth, CycleSummary, ExecutionMetric, EventType,
    HealthStatus, OptimizationRule, RuleCategory, SystemEvent,
    TaskDefinition, TaskPriority, TaskResult, TaskStatus,
)
from shared.unified_logging_service import AgentLoggingService
from execution.execution_data_collector import ExecutionDataCollector
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from execution.optimization_rule_repository import OptimizationRuleRepository
from core.core_task_execution_engine import CoreTaskExecutionEngine
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


def build_system(log_svc):
    """构建独立系统"""
    tmpdir = tempfile.mkdtemp(prefix="evo_deep_")
    recovery = ExceptionRecoveryHandler({}, log_svc)
    collector = ExecutionDataCollector(
        {"metrics_persistence_path": os.path.join(tmpdir, "m.json")}, log_svc
    )
    rule_repo = OptimizationRuleRepository(
        {"rules_persistence_path": os.path.join(tmpdir, "r.json")}, log_svc
    )
    engine = CoreTaskExecutionEngine({}, log_svc, collector, recovery)
    analyzer = IntelligentOptimizationAnalyzer({}, log_svc, collector, rule_repo, recovery)
    bridge = RuleSyncBridge(log_svc, rule_repo, engine, analyzer)
    guardian = ResidentGuardianManager({}, log_svc, recovery)
    return engine, collector, rule_repo, analyzer, bridge, guardian, recovery


class DeepTestResult:
    def __init__(self, name):
        self.name = name
        self.passed = False
        self.error = None
        self.duration = 0.0
    def __repr__(self):
        s = "✅" if self.passed else f"❌ {self.error}"
        return f"  [{s}] {self.name} ({self.duration:.3f}s)"


def run_deep_test(name, func, *args, **kwargs):
    result = DeepTestResult(name)
    start = time.time()
    try:
        func(*args, **kwargs)
        result.passed = True
    except AssertionError as e:
        result.error = str(e)
    except Exception as e:
        result.error = f"{type(e).__name__}: {e}"
    result.duration = time.time() - start
    return result


# ══════════════════════════════════════════════════════════════
#  深层测试场景
# ══════════════════════════════════════════════════════════════

def test_rule_apply_count_not_double_incremented():
    """深层测试1: mark_rule_applied不应导致apply_count双重递增"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    _, _, rule_repo, _, _, _, _ = build_system(log_svc)

    rule = OptimizationRule(name="count_test", category=RuleCategory.PERFORMANCE, action="test")
    rid = rule_repo.add_rule(rule)

    # 调用一次mark_rule_applied
    rule_repo.mark_rule_applied(rid)
    r = rule_repo.get_rule(rid)
    assert r is not None
    assert r.apply_count == 1, f"调用1次后apply_count应为1，实际{r.apply_count}"

    # 再调用一次
    rule_repo.mark_rule_applied(rid)
    r = rule_repo.get_rule(rid)
    assert r.apply_count == 2, f"调用2次后apply_count应为2，实际{r.apply_count}"

    print("    ✅ apply_count正确递增，无双重递增")


def test_failed_task_result_is_properly_returned():
    """深层测试2: 失败任务的result应正确返回（for循环return bug）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, _, _, _, _, _ = build_system(log_svc)

    def always_fail():
        raise RuntimeError("必定失败")

    engine.handler_registry.register("always_fail", always_fail)

    task = TaskDefinition(name="must_fail", handler="always_fail", max_retries=1)
    engine.submit_task(task)
    summary = engine.execute_cycle(1)

    assert summary.total_tasks == 1
    assert summary.failed_tasks == 1, f"应失败1个，实际{summary.failed_tasks}"

    # 验证采集器记录了这次失败
    metrics = collector.get_recent_metrics(1)
    assert len(metrics) == 1
    assert metrics[0].success == False, "采集器应记录为失败"
    assert metrics[0].error_type == "RuntimeError", f"错误类型应为RuntimeError，实际{metrics[0].error_type}"

    engine.shutdown()
    print("    ✅ 失败任务result正确返回")


def test_event_callback_should_not_block():
    """深层测试3: 事件回调中不应有阻塞操作"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    _, _, rule_repo, _, _, _, _ = build_system(log_svc)

    # 快速创建多条规则，触发事件回调
    start = time.time()
    for i in range(100):
        rule = OptimizationRule(name=f"fast_{i}", category=RuleCategory.PERFORMANCE, action="test")
        rule_repo.add_rule(rule)
    elapsed = time.time() - start

    # 如果每个事件回调都sleep(0.05)，100条规则至少需要5秒
    assert elapsed < 2.0, f"事件回调阻塞严重: 100条规则耗时{elapsed:.2f}s（应有防递归+无阻塞）"
    print(f"    ✅ 100条规则事件处理耗时{elapsed:.3f}s，无阻塞")


def test_concurrent_engine_shutdown_during_execution():
    """深层测试4: 执行过程中关闭引擎不应崩溃"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, _, _, _, _, _ = build_system(log_svc)

    def slow_task(duration_ms=500):
        time.sleep(duration_ms / 1000.0)
        return {"done": True}

    engine.handler_registry.register("slow_task", slow_task)

    # 提交大量慢任务
    tasks = [TaskDefinition(name=f"slow_{i}", handler="slow_task",
                             handler_params={"duration_ms": 500})
             for i in range(50)]
    engine.submit_tasks(tasks)

    # 在另一个线程中启动执行
    exec_thread = threading.Thread(target=engine.execute_cycle, args=(1,))
    exec_thread.start()

    # 等待任务开始执行后关闭
    time.sleep(0.2)
    try:
        engine.shutdown()
    except Exception as e:
        raise AssertionError(f"关闭期间崩溃: {e}")

    exec_thread.join(timeout=5)
    print("    ✅ 执行中关闭引擎不崩溃")


def test_cycle_summary_empty_queue_semantics():
    """深层测试5: 空队列周期的CycleSummary语义正确性"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, _, _, _, _, _ = build_system(log_svc)

    summary = engine.execute_cycle(1)
    assert summary.total_tasks == 0
    assert summary.completed_tasks == 0
    assert summary.failed_tasks == 0
    # 空周期success_rate=0.0是合理的（无数据），但不应导致除零错误
    assert summary.success_rate == 0.0
    assert summary.avg_duration == 0.0
    assert summary.duration >= 0

    engine.shutdown()
    print("    ✅ 空周期语义正确")


def test_data_collector_prune_preserves_recent():
    """深层测试6: 数据清理应保留最近数据"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    collector = ExecutionDataCollector({}, log_svc)

    # 模拟3个周期的数据
    for cycle in range(1, 4):
        for i in range(10):
            result = TaskResult(
                task_id=f"c{cycle}_t{i}",
                status=TaskStatus.COMPLETED,
                started_at=time.time() - 0.01,
                finished_at=time.time(),
            )
            collector.record_task_execution(result, cycle_number=cycle)

    summary = CycleSummary(cycle_number=3, total_tasks=10, completed_tasks=10)
    collector.record_cycle_summary(summary)

    # 清理：只保留最近1个周期
    removed = collector.prune_old_data(max_cycles=1)
    assert removed > 0, "应清理旧数据"

    stats = collector.get_total_stats()
    # 周期3的数据应保留
    assert stats["total_executions"] == 10, f"清理后应保留10条，实际{stats['total_executions']}"
    print(f"    ✅ 数据清理正确，移除{removed}条，保留{stats['total_executions']}条")


def test_optimization_analyzer_with_gradual_improvement():
    """深层测试7: 优化分析器应检测到逐步改善趋势"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, _, _, _ = build_system(log_svc)

    def sometimes_fail(fail_rate=0.5):
        import random
        if random.random() < fail_rate:
            raise RuntimeError("随机失败")
        return {"ok": True}

    engine.handler_registry.register("sometimes_fail", sometimes_fail)

    # 模拟逐渐改善：失败率从50%降到10%
    import random
    random.seed(42)

    for cycle in range(1, 11):
        fail_rate = max(0.1, 0.5 - cycle * 0.04)
        tasks = [TaskDefinition(
            name=f"imp_{cycle}_{i}",
            handler="sometimes_fail",
            handler_params={"fail_rate": fail_rate},
            max_retries=0,
        ) for i in range(20)]
        engine.submit_tasks(tasks)
        engine.execute_cycle(cycle)
        analyzer.analyze_cycle(cycle)

    stats = collector.get_total_stats()
    # 后期成功率应高于前期
    recent_rate = collector.get_success_rate(last_n=40)
    overall_rate = stats["overall_success_rate"]
    assert recent_rate >= overall_rate, \
        f"近期成功率({recent_rate:.2f})应>=整体({overall_rate:.2f})"

    # 分析器应已建立基线
    analysis_stats = analyzer.get_analysis_stats()
    assert analysis_stats["baseline_established"], "10个周期后应已建立基线"

    engine.shutdown()
    print(f"    ✅ 趋势分析正确: 整体成功率={overall_rate:.1%}, 近期={recent_rate:.1%}")


def test_sync_bridge_no_infinite_recursion():
    """深层测试8: 同步桥接器不应产生无限递归"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, _, _ = build_system(log_svc)

    # 快速创建+同步大量规则
    start = time.time()
    for i in range(50):
        rule = OptimizationRule(name=f"recursion_test_{i}", category=RuleCategory.PERFORMANCE, action="test")
        rule_repo.add_rule(rule)
        bridge.sync_all_rules()

    elapsed = time.time() - start
    assert elapsed < 5.0, f"同步可能存在递归: 50次耗时{elapsed:.2f}s"
    print(f"    ✅ 50次规则同步耗时{elapsed:.3f}s，无递归")


def test_guardian_with_rapid_health_fluctuation():
    """深层测试9: 守护层应对快速健康状态波动"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    _, _, _, _, _, guardian, _ = build_system(log_svc)

    flip_count = {"value": 0}
    restart_count = {"value": 0}

    def flipping_health():
        flip_count["value"] += 1
        if flip_count["value"] % 3 == 0:
            return ComponentHealth(component_name="flipper", status=HealthStatus.UNHEALTHY, error_count=10)
        return ComponentHealth(component_name="flipper", status=HealthStatus.HEALTHY)

    def fake_restart():
        restart_count["value"] += 1
        return True

    guardian.register_component("flipper", flipping_health, fake_restart, critical=True)

    # 快速连续检查20次
    for _ in range(20):
        guardian.check_all_components()

    # 重启次数应合理（不会每次UNHEALTHY都重启，因为有consecutive_failures计数）
    assert restart_count["value"] <= 7, f"重启次数过多: {restart_count['value']}"
    print(f"    ✅ 健康波动处理正确: 翻转{flip_count['value']}次，重启{restart_count['value']}次")


def test_engine_stats_consistency_after_partial_failure():
    """深层测试10: 部分失败后引擎统计一致性"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, _, _, _, _, _ = build_system(log_svc)

    def half_fail(n):
        if n % 2 == 0:
            raise ValueError("偶数失败")
        return {"n": n}

    engine.handler_registry.register("half_fail", half_fail)

    tasks = [TaskDefinition(name=f"hf_{i}", handler="half_fail",
                             handler_params={"n": i}, max_retries=0)
             for i in range(100)]
    engine.submit_tasks(tasks)
    summary = engine.execute_cycle(1)

    engine_stats = engine.get_stats()
    collector_stats = collector.get_total_stats()

    assert engine_stats["total_executed"] == 100
    assert collector_stats["total_executions"] == 100
    assert summary.completed_tasks == 50, f"应成功50个，实际{summary.completed_tasks}"
    assert summary.failed_tasks == 50, f"应失败50个，实际{summary.failed_tasks}"

    engine.shutdown()
    print(f"    ✅ 统计一致: 引擎={engine_stats['total_executed']}, 采集器={collector_stats['total_executions']}")


def test_multiple_persist_and_reload():
    """深层测试11: 多次持久化与重新加载一致性"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    tmpdir = tempfile.mkdtemp(prefix="evo_persist_")
    rules_path = os.path.join(tmpdir, "rules.json")
    metrics_path = os.path.join(tmpdir, "metrics.json")

    # 第一轮：创建数据并持久化
    _, collector1, rule_repo1, _, _, _, _ = build_system(log_svc)
    rule_repo1._persist_path = rules_path
    collector1._persist_path = metrics_path

    for i in range(10):
        rule = OptimizationRule(name=f"persist_{i}", category=RuleCategory.PERFORMANCE, action=f"act_{i}")
        rule_repo1.add_rule(rule)

    for i in range(20):
        result = TaskResult(task_id=f"m_{i}", status=TaskStatus.COMPLETED,
                            started_at=time.time(), finished_at=time.time())
        collector1.record_task_execution(result, cycle_number=1)

    rule_repo1.persist_to_file()
    collector1.persist_to_file()

    # 第二轮：从文件加载
    _, collector2, rule_repo2, _, _, _, _ = build_system(log_svc)
    rule_repo2._persist_path = rules_path
    collector2._persist_path = metrics_path
    rule_repo2._load_from_file()

    assert rule_repo2.get_rules_summary()["total_rules"] == 10, \
        f"重新加载后应有10条规则，实际{rule_repo2.get_rules_summary()['total_rules']}"

    # 第三轮：再添加并持久化
    for i in range(5):
        rule = OptimizationRule(name=f"new_{i}", category=RuleCategory.ERROR_REDUCTION, action=f"new_act_{i}")
        rule_repo2.add_rule(rule)
    rule_repo2.persist_to_file()

    # 第四轮：最终验证
    _, _, rule_repo3, _, _, _, _ = build_system(log_svc)
    rule_repo3._persist_path = rules_path
    rule_repo3._load_from_file()

    assert rule_repo3.get_rules_summary()["total_rules"] == 15, \
        f"最终应有15条规则，实际{rule_repo3.get_rules_summary()['total_rules']}"

    print("    ✅ 多轮持久化与重新加载一致")


def test_full_main_entry_with_custom_tasks():
    """深层测试12: 通过主入口运行完整系统"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    tmpdir = tempfile.mkdtemp(prefix="evo_full_")
    config = AgentConfig()
    config.set("storage.rules_persistence_path", os.path.join(tmpdir, "rules.json"))
    config.set("storage.metrics_persistence_path", os.path.join(tmpdir, "metrics.json"))
    config.set("logging.file_enabled", "False")
    config.set("logging.console_enabled", "False")
    config.set("execution.cycle_cooldown_seconds", 0)
    config.set("guardian.health_check_interval_seconds", 60)  # 长间隔避免干扰

    from evolution_agent_main_entry import EvolutionAgent
    agent = EvolutionAgent(config)
    agent.initialize()

    # 注册自定义handler
    def custom_task(value=0):
        return {"value": value * 2}

    agent._execution_engine.handler_registry.register("custom_task", custom_task)

    # 手动运行5个周期
    for cycle in range(1, 6):
        tasks = [TaskDefinition(name=f"custom_{i}", handler="custom_task",
                                 handler_params={"value": i})
                 for i in range(10)]
        agent._execution_engine.submit_tasks(tasks)
        agent._execution_engine.execute_cycle(cycle)
        agent._optimization_analyzer.analyze_cycle(cycle)

    stats = agent._execution_engine.get_stats()
    assert stats["total_executed"] == 50, f"应执行50个任务，实际{stats['total_executed']}"
    assert stats["total_success"] == 50, f"应全部成功，实际{stats['total_success']}"

    agent.shutdown()
    print(f"    ✅ 完整主入口运行正常: {stats['total_executed']}任务全部成功")


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 60)
    print("  🔬 持续进化Agent - 第二轮深层架构测试")
    print("=" * 60 + "\n")

    tests = [
        ("深层1: 规则apply_count无双重递增", test_rule_apply_count_not_double_incremented),
        ("深层2: 失败任务result正确返回", test_failed_task_result_is_properly_returned),
        ("深层3: 事件回调无阻塞", test_event_callback_should_not_block),
        ("深层4: 执行中关闭引擎不崩溃", test_concurrent_engine_shutdown_during_execution),
        ("深层5: 空周期语义正确", test_cycle_summary_empty_queue_semantics),
        ("深层6: 数据清理保留最近数据", test_data_collector_prune_preserves_recent),
        ("深层7: 优化分析器检测改善趋势", test_optimization_analyzer_with_gradual_improvement),
        ("深层8: 同步桥接器无无限递归", test_sync_bridge_no_infinite_recursion),
        ("深层9: 守护层快速健康波动", test_guardian_with_rapid_health_fluctuation),
        ("深层10: 部分失败后统计一致性", test_engine_stats_consistency_after_partial_failure),
        ("深层11: 多轮持久化与重新加载", test_multiple_persist_and_reload),
        ("深层12: 完整主入口运行", test_full_main_entry_with_custom_tasks),
    ]

    results = []
    for name, func in tests:
        print(f"  ▶ {name}...")
        result = run_deep_test(name, func)
        results.append(result)
        if result.passed:
            print(f"    ✅ 通过 ({result.duration:.3f}s)")
        else:
            print(f"    ❌ 失败: {result.error}")

    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    print(f"\n{'='*60}")
    print(f"  深层测试报告: {passed} 通过 / {failed} 失败 / {len(results)} 总计")
    print(f"{'='*60}")
    for r in results:
        print(r)
    if failed == 0:
        print(f"\n  🎉 全部深层测试通过")


if __name__ == "__main__":
    main()
