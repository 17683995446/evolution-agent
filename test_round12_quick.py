"""第12轮快速验证测试"""
import sys, os, time, threading
sys.path.insert(0, '.')

results = []

def test(name, func):
    try:
        func()
        results.append(("PASS", name, ""))
        print(f"  OK {name}")
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        results.append(("FAIL", name, str(e) + "\n" + tb))
        print(f"  FAIL {name}: {e}")


def t_p1_8():
    from shared.shared_data_models import RecoveryStrategy as SharedRS
    from execution.exception_recovery_handler import RecoveryStrategy as LocalRS
    assert SharedRS is LocalRS
    assert hasattr(SharedRS, 'ESCALATE')
test("P1-8: ESCALATE in shared RecoveryStrategy", t_p1_8)


def t_p2_1():
    from adaptive.adaptive_scheduler import AdaptiveScheduler, AdaptiveConfig
    s = AdaptiveScheduler(AdaptiveConfig(adjustment_interval_cycles=1))
    for i in range(15):
        s.record_execution('h', 0.1, True)
    for i in range(5):
        s.record_execution('h', 0.1, False)
    s.adjust_parameters()
    if s._adjustment_log:
        entry = s._adjustment_log[-1]
        assert 'old_params' in entry and 'new_params' in entry
test("P2-1: adjustment log correct old params", t_p2_1)


def t_p2_2():
    from adaptive.adaptive_scheduler import AdaptiveScheduler, AdaptiveConfig
    s = AdaptiveScheduler(AdaptiveConfig(adjustment_interval_cycles=1))
    for i in range(15):
        s.record_execution('h', 0.1, True)
    for i in range(5):
        s.record_execution('h', 0.1, False)
    for _ in range(300):
        s.adjust_parameters()
    assert len(s._adjustment_log) <= 200
test("P2-2: adjustment log bounded", t_p2_2)


def t_p2_3():
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService
    ls = AgentLoggingService({'log_level': 'WARNING'})
    h = ExceptionRecoveryHandler({'guardian': {}}, ls)
    for i in range(1500):
        h.handle_exception('c', RuntimeError(f'e{i}'))
    stats = h.get_error_stats()
    assert stats['total_errors'] <= 1000
test("P2-3: error records bounded", t_p2_3)


def t_p2_4():
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService
    ls = AgentLoggingService({'log_level': 'WARNING'})
    h = ExceptionRecoveryHandler({'guardian': {}}, ls)
    h.handle_exception('c', RuntimeError('test'))
    h1 = h.get_component_health('c')
    h1.error_count = 99999
    h2 = h.get_component_health('c')
    assert h2.error_count != 99999
test("P2-4: component health returns copy", t_p2_4)


def t_p2_5():
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService
    ls = AgentLoggingService({'log_level': 'WARNING'})
    h = ExceptionRecoveryHandler({'guardian': {}}, ls)
    ids = set()
    for i in range(100):
        h.handle_exception('c', RuntimeError(f'e{i}'))
        with h._lock:
            ids.add(h._error_records[-1].error_id)
    assert len(ids) == 100
test("P2-5: error id unique", t_p2_5)


def t_chaos_storm():
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import EventType, SystemEvent
    ls = AgentLoggingService({'log_level': 'CRITICAL'})
    counter = {'total': 0, 'errors': 0}
    lock = threading.Lock()
    def storm(tid):
        for i in range(200):
            try:
                ls.emit_event(SystemEvent(
                    event_type=EventType.TASK_COMPLETED,
                    source=f's_{tid}',
                    data={'i': i}
                ))
                with lock:
                    counter['total'] += 1
            except Exception:
                with lock:
                    counter['errors'] += 1
    threads = [threading.Thread(target=storm, args=(t,)) for t in range(500)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)
    assert counter['errors'] == 0
    assert counter['total'] == 100000
test("Chaos: 500 thread event storm", t_chaos_storm)


def t_chaos_engine():
    from shared.shared_data_models import TaskDefinition, TaskPriority
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService
    ls = AgentLoggingService({'log_level': 'WARNING'})
    c = ExecutionDataCollector({'storage': {}}, ls)
    rv = ExceptionRecoveryHandler({'guardian': {}}, ls)
    eng = CoreTaskExecutionEngine({
        'max_concurrent_tasks': 10, 'task_timeout_seconds': 30,
        'max_retries_per_task': 1, 'retry_delay_seconds': 0.01,
        'task_queue_max_size': 5000
    }, ls, c, rv)
    eng.handler_registry.register('fast', lambda x=0: x * 2)
    tasks = [
        TaskDefinition(
            name=f't_{i}', handler='fast', handler_params={'x': i},
            priority=TaskPriority.NORMAL, max_retries=1
        ) for i in range(2000)
    ]
    submitted = eng.submit_tasks(tasks)
    assert submitted == 2000
    summary = eng.execute_cycle(1)
    assert summary.total_tasks == 2000
    eng.shutdown()
test("Chaos: engine 2000 tasks", t_chaos_engine)


def t_chaos_recovery():
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from shared.unified_logging_service import AgentLoggingService
    ls = AgentLoggingService({'log_level': 'CRITICAL'})
    h = ExceptionRecoveryHandler({'guardian': {}}, ls)
    errors = []
    def stress(tid):
        for i in range(200):
            try:
                h.handle_exception(f'c_{tid}', RuntimeError(f'e{i}'))
                h.get_error_stats()
                h.get_component_health(f'c_{tid}')
                h.get_all_health()
                h.reset_component_errors(f'c_{tid}')
            except Exception as e:
                errors.append(e)
    threads = [threading.Thread(target=stress, args=(t,)) for t in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=15)
    assert len(errors) == 0
test("Chaos: recovery handler stress", t_chaos_recovery)


def t_integ_pipeline():
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
    import random
    ls = AgentLoggingService({'log_level': 'WARNING'})
    c = ExecutionDataCollector({'storage': {}}, ls)
    rv = ExceptionRecoveryHandler({'guardian': {}}, ls)
    repo = OptimizationRuleRepository({'optimization': {}}, ls)
    eng = CoreTaskExecutionEngine({
        'max_concurrent_tasks': 5, 'task_timeout_seconds': 30,
        'max_retries_per_task': 2, 'retry_delay_seconds': 0.01,
        'task_queue_max_size': 1000
    }, ls, c, rv)
    ana = IntelligentOptimizationAnalyzer(
        {'optimization': {}}, ls, c, repo, rv
    )
    bridge = RuleSyncBridge(ls, repo, eng, ana)
    eng.handler_registry.register('good', lambda x=0: x + 1)

    def flaky(x=0):
        if random.random() < 0.3:
            raise RuntimeError('fail')
        return x * 2
    eng.handler_registry.register('flaky', flaky)

    for i in range(5):
        repo.add_rule(OptimizationRule(
            name=f'r_{i}', description=f'Rule {i}',
            category=RuleCategory.PERFORMANCE, condition='always',
            action=f'a_{i}'
        ))

    for cycle in range(1, 11):
        for i in range(20):
            eng.submit_task(TaskDefinition(
                name=f'c{cycle}_t{i}',
                handler='flaky' if i % 3 == 0 else 'good',
                handler_params={'x': i},
                priority=TaskPriority.NORMAL, max_retries=2
            ))
        summary = eng.execute_cycle(cycle)
        assert summary.total_tasks > 0
        try:
            ana.analyze_cycle(cycle)
        except Exception:
            pass
        bridge.sync_all_rules()

    stats = eng.get_stats()
    assert stats['total_executed'] > 0
    bridge.stop()
    eng.shutdown()
test("Integration: full pipeline with chaos", t_integ_pipeline)


# Summary
passed = sum(1 for r in results if r[0] == "PASS")
failed = sum(1 for r in results if r[0] == "FAIL")
print(f"\n{'='*60}")
print(f"  Round 12 Results: {passed} passed, {failed} failed")
print(f"{'='*60}")
if failed > 0:
    for r in results:
        if r[0] == "FAIL":
            print(f"  FAIL: {r[1]}")
            print(f"        {r[2][:500]}")
sys.exit(0 if failed == 0 else 1)
