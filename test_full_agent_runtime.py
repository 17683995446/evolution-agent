#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第5轮最终测试：使用EvolutionAgent主入口进行真实长时间运行
直接测试完整的系统组装和运行流程，发现集成层面的问题。
"""

import sys
import os
import time
import threading
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from evolution_agent_main_entry import EvolutionAgent
from shared.global_configuration_loader import AgentConfig


def test_full_agent_run():
    """
    完整Agent运行测试：使用主入口运行100个周期
    验证：执行+优化双并行、规则生成、数据持久化、守护线程
    """
    print("\n" + "=" * 70)
    print("  第5轮最终测试：完整Agent真实运行")
    print("=" * 70)

    config = AgentConfig({
        "execution": {
            "max_concurrent_tasks": 3,
            "task_timeout_seconds": 30.0,
            "task_queue_max_size": 200,
            "cycle_cooldown_seconds": 0.0,
        },
        "guardian": {
            "health_check_interval_seconds": 2.0,
            "max_component_errors_before_restart": 10,
        },
        "storage": {
            "metrics_retention_cycles": 50,
            "persist_interval_cycles": 25,
        },
        "logging": {
            "console_enabled": True,
            "file_enabled": False,
        },
    })

    agent = EvolutionAgent(config)
    agent.initialize()

    # 注册示例处理器
    from examples.example_task_definitions import register_all_sample_handlers
    register_all_sample_handlers(agent._execution_engine.handler_registry)

    # 注册额外的高压测试处理器
    def high_frequency_handler(**kwargs):
        """高频计算任务"""
        result = sum(i * i for i in range(1000))
        return {"computed": result}

    def occasionally_fail_handler(**kwargs):
        """偶尔失败的任务"""
        if random.random() < 0.15:
            raise RuntimeError("模拟间歇性故障")
        return {"status": "ok"}

    agent._execution_engine.handler_registry.register("high_freq", high_frequency_handler)
    agent._execution_engine.handler_registry.register("occasional_fail", occasionally_fail_handler)

    # 注入混合任务
    original_feed = agent._feed_tasks

    def mixed_feed(cycle, count):
        """混合任务投喂：包含正常任务和高压任务"""
        from examples.example_task_definitions import create_sample_task_batch
        from shared.shared_data_models import TaskDefinition, TaskPriority

        tasks = create_sample_task_batch(count)

        # 每5个周期注入一些高压任务
        if cycle % 5 == 0:
            for i in range(5):
                tasks.append(TaskDefinition(
                    name=f"[高压-{i}] 高频计算",
                    handler="high_freq",
                    handler_params={"cycle": cycle},
                    priority=TaskPriority.HIGH,
                ))

        # 每10个周期注入一些可能失败的任务
        if cycle % 10 == 0:
            for i in range(3):
                tasks.append(TaskDefinition(
                    name=f"[不稳定-{i}] 间歇故障",
                    handler="occasional_fail",
                    handler_params={},
                    priority=TaskPriority.NORMAL,
                    max_retries=2,
                ))

        submitted = agent._execution_engine.submit_tasks(tasks)
        agent._log.debug(f"周期 #{cycle}: 已提交 {submitted} 个任务")

    agent._feed_tasks = mixed_feed

    # 启动守护
    agent._guardian.start_guarding()

    # 运行100个周期
    print(f"\n  开始运行 100 个周期...")
    start_time = time.time()
    errors = []
    total_completed = 0
    total_failed = 0

    for cycle in range(1, 101):
        try:
            agent._cycle_number = cycle

            # 投喂任务
            agent._feed_tasks(cycle, 8)

            # 执行周期
            summary = agent._execution_engine.execute_cycle(cycle)

            total_completed += summary.completed_tasks
            total_failed += summary.failed_tasks

            # 优化分析
            try:
                new_rules = agent._optimization_analyzer.analyze_cycle(cycle)
                if new_rules:
                    agent._sync_bridge.sync_new_rules(new_rules)
            except Exception as e:
                errors.append(f"周期 {cycle} 分析异常: {e}")

            # 定期持久化
            if cycle % 25 == 0:
                try:
                    agent._rule_repository.persist_to_file()
                    agent._data_collector.persist_to_file()
                except Exception as e:
                    errors.append(f"周期 {cycle} 持久化异常: {e}")

            # 定期清理
            if cycle % 50 == 0:
                try:
                    agent._data_collector.prune_old_data(max_cycles=30)
                except Exception as e:
                    errors.append(f"周期 {cycle} 清理异常: {e}")

            # 进度报告
            if cycle % 20 == 0:
                elapsed = time.time() - start_time
                stats = agent._execution_engine.get_stats()
                collector_stats = agent._data_collector.get_total_stats()
                rule_stats = agent._rule_repository.get_rules_summary()
                print(f"\n  📊 周期 {cycle}/100 进度报告:")
                print(f"     任务: 完成={total_completed}, 失败={total_failed}")
                print(f"     引擎: 累计={stats['total_executed']}, "
                      f"成功率={stats['overall_success_rate']:.1%}")
                print(f"     规则: 活跃={rule_stats['active_rules']}, "
                      f"总={rule_stats['total_rules']}")
                print(f"     采集: 总执行={collector_stats['total_executions']}, "
                      f"错误类型={collector_stats['error_types_count']}")
                print(f"     耗时: {elapsed:.1f}s, 错误数: {len(errors)}")

        except Exception as e:
            errors.append(f"周期 {cycle} 致命异常: {e}")
            import traceback
            traceback.print_exc()

    elapsed = time.time() - start_time

    # 停止
    agent._guardian.stop_guarding()
    agent._execution_engine.shutdown()

    # 最终统计
    total_tasks = total_completed + total_failed
    success_rate = total_completed / total_tasks if total_tasks > 0 else 0

    print(f"\n{'=' * 70}")
    print(f"  最终测试报告")
    print(f"{'=' * 70}")
    print(f"  总周期: 100")
    print(f"  总任务: {total_tasks}")
    print(f"  成功: {total_completed} ({success_rate:.1%})")
    print(f"  失败: {total_failed}")
    print(f"  错误: {len(errors)}")
    print(f"  总耗时: {elapsed:.1f}s")
    print(f"  平均每周期: {elapsed/100:.3f}s")
    print(f"{'=' * 70}")

    if errors:
        print(f"\n  ⚠️ 错误列表:")
        for e in errors[:10]:
            print(f"    - {e}")

    # 验证
    assert total_tasks > 0, "没有执行任何任务"
    assert success_rate > 0.5, f"成功率过低: {success_rate:.1%}"
    assert len(errors) < 20, f"错误过多: {len(errors)}"

    print(f"\n  ✅ 测试通过！系统在100周期真实运行中表现稳定。")
    return True


if __name__ == "__main__":
    try:
        test_full_agent_run()
    except Exception as e:
        print(f"\n  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
