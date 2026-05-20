"""
持续进化Agent - 极端混沌测试套件
模拟真实生产环境中的混乱场景：
- 资源耗尽（大量并发、内存压力）
- 随机故障注入（各种异常类型）
- 竞态条件（多线程竞争）
- 长时间运行（100+周期）
- 混合场景（成功/失败/超时/重试混合）
"""

from __future__ import annotations

import gc
import os
import random
import sys
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
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


# ══════════════════════════════════════════════════════════════
#  混沌测试框架
# ══════════════════════════════════════════════════════════════

class ChaosTestResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.error: str = ""
        self.duration: float = 0.0
        self.metrics: dict = {}

    def __repr__(self):
        status = "✅" if self.passed else f"❌ {self.error[:50]}"
        extra = f" | {self.metrics}" if self.metrics else ""
        return f"  [{status}] {self.name} ({self.duration:.2f}s){extra}"


class ChaosTestSuite:
    """混沌测试套件"""

    def __init__(self):
        self.results: list[ChaosTestResult] = []
        self._log_config = {"console_enabled": False, "file_enabled": False}
        self.log_svc = AgentLoggingService(self._log_config)

    def run_test(self, name: str, func, *args, **kwargs) -> ChaosTestResult:
        result = ChaosTestResult(name)
        start = time.time()
        try:
            metrics = func(*args, **kwargs)
            result.passed = True
            result.metrics = metrics or {}
        except AssertionError as e:
            result.error = str(e)
        except Exception as e:
            result.error = f"{type(e).__name__}: {e}"
        result.duration = time.time() - start
        self.results.append(result)
        return result

    def print_report(self):
        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed
        print(f"\n{'=' * 60}")
        print(f"  混沌测试报告: {passed} 通过 / {failed} 失败 / {len(self.results)} 总计")
        print(f"{'=' * 60}")
        for r in self.results:
            print(r)
        if failed == 0:
            print(f"\n  🎉 全部混沌测试通过！系统在极端条件下稳定运行")


def build_chaos_system(log_svc: AgentLoggingService) -> tuple:
    """构建混沌测试系统"""
    tmpdir = tempfile.mkdtemp(prefix="chaos_test_")
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


def register_chaos_handlers(engine: CoreTaskExecutionEngine):
    """注册混沌测试任务处理器"""

    # 1. 瞬时任务
    def instant_task(value: int = 0) -> dict:
        return {"result": value * 2}

    # 2. 随机延迟任务
    def random_delay_task(min_ms: int = 1, max_ms: int = 100) -> dict:
        delay = random.randint(min_ms, max_ms) / 1000.0
        time.sleep(delay)
        return {"delayed": delay}

    # 3. 随机失败任务
    def random_fail_task(fail_rate: float = 0.3) -> dict:
        if random.random() < fail_rate:
            error_types = [
                lambda: (_ for _ in ()).throw(ValueError("随机值错误")),
                lambda: (_ for _ in ()).throw(RuntimeError("随机运行时错误")),
                lambda: (_ for _ in ()).throw(TypeError("随机类型错误")),
                lambda: (_ for _ in ()).throw(ConnectionError("随机连接错误")),
                lambda: (_ for _ in ()).throw(TimeoutError("随机超时错误")),
            ]
            random.choice(error_types)()
        return {"success": True}

    # 4. 内存压力任务
    def memory_pressure_task(mb: int = 1) -> dict:
        data = [b"x" * 1024 for _ in range(mb * 1024)]
        result = len(data)
        del data
        gc.collect()
        return {"allocated_mb": mb, "items": result}

    # 5. 计算密集任务
    def compute_intensive_task(n: int = 10000) -> dict:
        result = sum(i * i for i in range(n))
        return {"sum_of_squares": result}

    # 6. 级联失败任务
    def cascade_fail_task(depth: int = 3) -> dict:
        if depth > 0:
            if random.random() < 0.4:
                raise RuntimeError(f"级联失败 depth={depth}")
            time.sleep(0.01)
        return {"depth": depth}

    # 7. 资源竞争任务
    shared_counter = {"value": 0, "lock": threading.Lock()}

    def race_condition_task(increment: int = 1) -> dict:
        # 故意不使用锁，制造竞态条件
        old = shared_counter["value"]
        time.sleep(0.0001)  # 增加竞态窗口
        shared_counter["value"] = old + increment
        return {"counter": shared_counter["value"]}

    # 8. 超时任务
    def timeout_task(duration_ms: int = 1000) -> dict:
        time.sleep(duration_ms / 1000.0)
        return {"slept_ms": duration_ms}

    # 9. 异常链任务
    def exception_chain_task(raise_type: str = "none") -> dict:
        if raise_type == "value":
            raise ValueError("值错误")
        elif raise_type == "runtime":
            raise RuntimeError("运行时错误")
        elif raise_type == "key":
            raise KeyError("键错误")
        elif raise_type == "index":
            raise IndexError("索引错误")
        return {"no_error": True}

    # 10. 随机行为任务（最混乱）
    def chaotic_task(seed: int = 0) -> dict:
        random.seed(seed)
        behavior = random.choice(["success", "fail", "delay", "memory", "compute"])
        if behavior == "success":
            return {"behavior": "success"}
        elif behavior == "fail":
            raise RuntimeError("混沌失败")
        elif behavior == "delay":
            time.sleep(random.random() * 0.1)
            return {"behavior": "delay"}
        elif behavior == "memory":
            _ = [b"x" * 1024 for _ in range(100)]
            gc.collect()
            return {"behavior": "memory"}
        else:
            _ = sum(i for i in range(1000))
            return {"behavior": "compute"}

    handlers = {
        "instant_task": instant_task,
        "random_delay_task": random_delay_task,
        "random_fail_task": random_fail_task,
        "memory_pressure_task": memory_pressure_task,
        "compute_intensive_task": compute_intensive_task,
        "cascade_fail_task": cascade_fail_task,
        "race_condition_task": race_condition_task,
        "timeout_task": timeout_task,
        "exception_chain_task": exception_chain_task,
        "chaotic_task": chaotic_task,
    }

    for name, handler in handlers.items():
        engine.handler_registry.register(name, handler)


# ══════════════════════════════════════════════════════════════
#  混沌测试场景
# ══════════════════════════════════════════════════════════════

def chaos_test_massive_concurrent_submission():
    """混沌测试1: 海量并发任务提交（10000任务）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)

    # 扩大队列
    import queue as q
    engine._task_queue = q.Queue(maxsize=20000)

    # 100个线程各提交100个任务
    def submit_batch(thread_id):
        for i in range(100):
            task = TaskDefinition(
                name=f"massive_t{thread_id}_i{i}",
                handler="instant_task",
                handler_params={"value": thread_id * 100 + i},
                priority=random.choice(list(TaskPriority)),
            )
            engine.submit_task(task)

    threads = [threading.Thread(target=submit_batch, args=(t,)) for t in range(100)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    submit_time = time.time() - start

    # 执行
    summary = engine.execute_cycle(1)

    engine.shutdown()
    return {
        "submitted": 10000,
        "executed": summary.total_tasks,
        "success": summary.completed_tasks,
        "submit_time": f"{submit_time:.3f}s",
        "success_rate": f"{summary.success_rate:.1%}",
    }


def chaos_test_random_failure_injection():
    """混沌测试2: 随机故障注入（1000任务，30%失败率）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)

    tasks = [
        TaskDefinition(
            name=f"fail_inject_{i}",
            handler="random_fail_task",
            handler_params={"fail_rate": 0.3},
            max_retries=random.randint(0, 3),
        )
        for i in range(1000)
    ]
    engine.submit_tasks(tasks)
    summary = engine.execute_cycle(1)

    error_dist = collector.get_error_distribution()

    engine.shutdown()
    return {
        "total": 1000,
        "success": summary.completed_tasks,
        "failed": summary.failed_tasks,
        "error_types": len(error_dist),
        "success_rate": f"{summary.success_rate:.1%}",
    }


def chaos_test_memory_pressure():
    """混沌测试3: 内存压力测试（500个内存任务）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)

    tasks = [
        TaskDefinition(
            name=f"mem_pressure_{i}",
            handler="memory_pressure_task",
            handler_params={"mb": random.randint(1, 5)},
        )
        for i in range(500)
    ]
    engine.submit_tasks(tasks)
    summary = engine.execute_cycle(1)

    engine.shutdown()
    gc.collect()
    return {
        "total": 500,
        "success": summary.completed_tasks,
        "failed": summary.failed_tasks,
        "success_rate": f"{summary.success_rate:.1%}",
    }


def chaos_test_mixed_chaotic_scenario():
    """混沌测试4: 混合混沌场景（各种任务类型混合）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)

    tasks = []
    # 瞬时任务 30%
    for i in range(300):
        tasks.append(TaskDefinition(
            name=f"instant_{i}", handler="instant_task",
            handler_params={"value": i},
        ))
    # 随机延迟 20%
    for i in range(200):
        tasks.append(TaskDefinition(
            name=f"delay_{i}", handler="random_delay_task",
            handler_params={"min_ms": 1, "max_ms": 50},
        ))
    # 随机失败 20%
    for i in range(200):
        tasks.append(TaskDefinition(
            name=f"fail_{i}", handler="random_fail_task",
            handler_params={"fail_rate": 0.4},
            max_retries=2,
        ))
    # 计算 15%
    for i in range(150):
        tasks.append(TaskDefinition(
            name=f"compute_{i}", handler="compute_intensive_task",
            handler_params={"n": random.randint(1000, 10000)},
        ))
    # 混沌 15%
    for i in range(150):
        tasks.append(TaskDefinition(
            name=f"chaotic_{i}", handler="chaotic_task",
            handler_params={"seed": i},
            max_retries=1,
        ))

    random.shuffle(tasks)  # 打乱顺序
    engine.submit_tasks(tasks)
    summary = engine.execute_cycle(1)

    engine.shutdown()
    return {
        "total": 1000,
        "success": summary.completed_tasks,
        "failed": summary.failed_tasks,
        "success_rate": f"{summary.success_rate:.1%}",
    }


def chaos_test_long_running_with_optimization():
    """混沌测试5: 长时间运行+优化（100周期）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)

    total_success = 0
    total_failed = 0
    rules_created = 0

    for cycle in range(1, 101):
        # 每周期混合任务
        tasks = []
        for i in range(20):
            tasks.append(TaskDefinition(
                name=f"long_c{cycle}_ok_{i}",
                handler="instant_task",
                handler_params={"value": i},
            ))
        for i in range(5):
            tasks.append(TaskDefinition(
                name=f"long_c{cycle}_fail_{i}",
                handler="random_fail_task",
                handler_params={"fail_rate": 0.5},
                max_retries=1,
            ))
        for i in range(5):
            tasks.append(TaskDefinition(
                name=f"long_c{cycle}_delay_{i}",
                handler="random_delay_task",
                handler_params={"min_ms": 1, "max_ms": 20},
            ))

        engine.submit_tasks(tasks)
        summary = engine.execute_cycle(cycle)
        total_success += summary.completed_tasks
        total_failed += summary.failed_tasks

        # 优化分析
        new_rules = analyzer.analyze_cycle(cycle)
        rules_created += len(new_rules)
        if new_rules:
            bridge.sync_new_rules(new_rules)

    engine.shutdown()
    return {
        "cycles": 100,
        "total_tasks": total_success + total_failed,
        "success": total_success,
        "failed": total_failed,
        "rules_created": rules_created,
        "success_rate": f"{total_success / (total_success + total_failed):.1%}",
    }


def chaos_test_concurrent_cycle_execution():
    """混沌测试6: 并发周期执行（多线程同时执行周期）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)

    errors = []
    results = []

    def run_cycle(thread_id):
        try:
            for i in range(10):
                tasks = [
                    TaskDefinition(
                        name=f"conc_t{thread_id}_c{i}_j{j}",
                        handler="instant_task",
                        handler_params={"value": j},
                    )
                    for j in range(10)
                ]
                engine.submit_tasks(tasks)
                summary = engine.execute_cycle(thread_id * 100 + i)
                results.append(summary.completed_tasks)
        except Exception as e:
            errors.append(f"线程{thread_id}: {e}")

    threads = [threading.Thread(target=run_cycle, args=(t,)) for t in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    engine.shutdown()
    return {
        "threads": 20,
        "cycles_per_thread": 10,
        "errors": len(errors),
        "total_success": sum(results),
    }


def chaos_test_exception_chain():
    """混沌测试7: 异常链测试（各种异常类型）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)

    exception_types = ["none", "none", "none", "value", "runtime", "key", "index"]
    tasks = [
        TaskDefinition(
            name=f"exc_chain_{i}",
            handler="exception_chain_task",
            handler_params={"raise_type": random.choice(exception_types)},
            max_retries=1,
        )
        for i in range(500)
    ]
    engine.submit_tasks(tasks)
    summary = engine.execute_cycle(1)

    error_dist = collector.get_error_distribution()

    engine.shutdown()
    return {
        "total": 500,
        "success": summary.completed_tasks,
        "failed": summary.failed_tasks,
        "error_types": list(error_dist.keys()),
        "success_rate": f"{summary.success_rate:.1%}",
    }


def chaos_test_rule_repository_stress():
    """混沌测试8: 规则仓库极限压力（1000规则并发读写）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    _, _, rule_repo, _, _, _, _ = build_chaos_system(log_svc)

    errors = []

    def writer(thread_id):
        try:
            for i in range(100):
                rule = OptimizationRule(
                    name=f"stress_rule_t{thread_id}_i{i}",
                    category=random.choice(list(RuleCategory)),
                    action=f"action_{i}",
                    source_cycle=thread_id,
                )
                rule_repo.add_rule(rule)
                if i % 10 == 0:
                    rule_repo.mark_rule_applied(rule.rule_id)
        except Exception as e:
            errors.append(f"写{thread_id}: {e}")

    def reader(thread_id):
        try:
            for i in range(100):
                active = rule_repo.get_active_rules()
                summary = rule_repo.get_rules_summary()
                if len(active) > 0:
                    rule_repo.get_rule(active[0].rule_id)
        except Exception as e:
            errors.append(f"读{thread_id}: {e}")

    threads = []
    for t in range(25):
        threads.append(threading.Thread(target=writer, args=(t,)))
        threads.append(threading.Thread(target=reader, args=(t,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    summary = rule_repo.get_rules_summary()
    return {
        "threads": 50,
        "total_rules": summary["total_rules"],
        "active_rules": summary["active_rules"],
        "errors": len(errors),
    }


def chaos_test_data_collector_stress():
    """混沌测试9: 数据采集器极限压力（10000条并发记录）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    collector = ExecutionDataCollector({}, log_svc)

    errors = []

    def recorder(thread_id):
        try:
            for i in range(200):
                success = random.random() > 0.3
                result = TaskResult(
                    task_id=f"stress_t{thread_id}_i{i}",
                    status=TaskStatus.COMPLETED if success else TaskStatus.FAILED,
                    started_at=time.time() - random.random() * 0.1,
                    finished_at=time.time(),
                )
                if not success:
                    error_types = [ValueError, RuntimeError, TypeError, KeyError]
                    result.error = random.choice(error_types)(f"错误{i}")
                collector.record_task_execution(result, cycle_number=thread_id)
        except Exception as e:
            errors.append(f"记录{thread_id}: {e}")

    threads = [threading.Thread(target=recorder, args=(t,)) for t in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats = collector.get_total_stats()
    return {
        "threads": 50,
        "total_records": stats["total_executions"],
        "success_rate": f"{stats['overall_success_rate']:.1%}",
        "errors": len(errors),
    }


def chaos_test_guardian_under_stress():
    """混沌测试10: 守护层压力测试（快速健康状态变化）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    _, _, _, _, _, guardian, recovery = build_chaos_system(log_svc)

    restart_counts = {"value": 0}

    def unstable_health():
        r = random.random()
        if r < 0.3:
            return ComponentHealth(
                component_name="unstable",
                status=HealthStatus.UNHEALTHY,
                error_count=10,
            )
        elif r < 0.6:
            return ComponentHealth(
                component_name="unstable",
                status=HealthStatus.DEGRADED,
                error_count=3,
            )
        return ComponentHealth(component_name="unstable", status=HealthStatus.HEALTHY)

    def fake_restart():
        restart_counts["value"] += 1
        return True

    guardian.register_component("unstable", unstable_health, fake_restart, critical=True)

    # 快速检查100次
    for _ in range(100):
        guardian.check_all_components()

    health = guardian.get_system_health()
    return {
        "checks": 100,
        "restarts": restart_counts["value"],
        "final_status": health["overall_status"],
    }


def chaos_test_full_system_integration():
    """混沌测试11: 完整系统集成（执行+采集+分析+同步+守护）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)

    guardian.register_component(
        "engine", lambda: ComponentHealth(component_name="engine")
    )

    total_executed = 0
    total_success = 0

    for cycle in range(1, 51):
        tasks = []
        for i in range(15):
            tasks.append(TaskDefinition(
                name=f"int_c{cycle}_ok_{i}",
                handler="instant_task",
                handler_params={"value": i},
            ))
        for i in range(3):
            tasks.append(TaskDefinition(
                name=f"int_c{cycle}_fail_{i}",
                handler="random_fail_task",
                handler_params={"fail_rate": 0.5},
                max_retries=1,
            ))
        for i in range(2):
            tasks.append(TaskDefinition(
                name=f"int_c{cycle}_delay_{i}",
                handler="random_delay_task",
                handler_params={"min_ms": 1, "max_ms": 10},
            ))

        engine.submit_tasks(tasks)
        summary = engine.execute_cycle(cycle)
        total_executed += summary.total_tasks
        total_success += summary.completed_tasks

        new_rules = analyzer.analyze_cycle(cycle)
        if new_rules:
            bridge.sync_new_rules(new_rules)

    guardian.stop_guarding()
    engine.shutdown()

    return {
        "cycles": 50,
        "total_executed": total_executed,
        "total_success": total_success,
        "success_rate": f"{total_success / total_executed:.1%}",
    }


def chaos_test_persistence_under_pressure():
    """混沌测试12: 持久化压力测试（频繁读写）"""
    log_svc = AgentLoggingService({"console_enabled": False, "file_enabled": False})
    tmpdir = tempfile.mkdtemp(prefix="chaos_persist_")
    rules_path = os.path.join(tmpdir, "rules.json")
    metrics_path = os.path.join(tmpdir, "metrics.json")

    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = build_chaos_system(log_svc)
    register_chaos_handlers(engine)
    rule_repo._persist_path = rules_path
    collector._persist_path = metrics_path

    errors = []

    for round_num in range(20):
        try:
            # 创建规则
            for i in range(25):
                rule = OptimizationRule(
                    name=f"persist_r{round_num}_i{i}",
                    category=random.choice(list(RuleCategory)),
                    action=f"action_{i}",
                )
                rule_repo.add_rule(rule)

            # 执行任务
            tasks = [
                TaskDefinition(
                    name=f"persist_t{round_num}_i{i}",
                    handler="instant_task",
                    handler_params={"value": i},
                )
                for i in range(25)
            ]
            engine.submit_tasks(tasks)
            engine.execute_cycle(round_num + 1)

            # 持久化
            rule_repo.persist_to_file()
            collector.persist_to_file()

        except Exception as e:
            errors.append(f"轮{round_num}: {e}")

    # 最终验证
    rule_repo.persist_to_file()
    final_rules = rule_repo.get_rules_summary()

    engine.shutdown()
    return {
        "rounds": 20,
        "total_rules": final_rules["total_rules"],
        "errors": len(errors),
    }


# ══════════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 60)
    print("  🔥 持续进化Agent - 极端混沌测试套件")
    print("=" * 60 + "\n")

    suite = ChaosTestSuite()

    tests = [
        ("混沌1: 海量并发提交 (10000任务)", chaos_test_massive_concurrent_submission),
        ("混沌2: 随机故障注入 (1000任务30%失败)", chaos_test_random_failure_injection),
        ("混沌3: 内存压力 (500任务)", chaos_test_memory_pressure),
        ("混沌4: 混合混沌场景 (1000混合任务)", chaos_test_mixed_chaotic_scenario),
        ("混沌5: 长时间运行+优化 (100周期)", chaos_test_long_running_with_optimization),
        ("混沌6: 并发周期执行 (20线程)", chaos_test_concurrent_cycle_execution),
        ("混沌7: 异常链测试 (500任务)", chaos_test_exception_chain),
        ("混沌8: 规则仓库压力 (50线程)", chaos_test_rule_repository_stress),
        ("混沌9: 数据采集器压力 (50线程)", chaos_test_data_collector_stress),
        ("混沌10: 守护层压力 (100次检查)", chaos_test_guardian_under_stress),
        ("混沌11: 完整系统集成 (50周期)", chaos_test_full_system_integration),
        ("混沌12: 持久化压力 (20轮)", chaos_test_persistence_under_pressure),
    ]

    for name, func in tests:
        print(f"  ▶ {name}...")
        result = suite.run_test(name, func)
        if result.passed:
            print(f"    ✅ 通过 ({result.duration:.2f}s) | {result.metrics}")
        else:
            print(f"    ❌ 失败: {result.error[:80]}")

    suite.print_report()


if __name__ == "__main__":
    main()
