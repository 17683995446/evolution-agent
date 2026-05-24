#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第7轮：生产级真实运行测试
直接使用 EvolutionAgent 主入口，注入多层混乱，发现集成层面BUG。
"""

import sys
import os
import time
import threading
import random
import traceback
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from evolution_agent_main_entry import EvolutionAgent
from shared.global_configuration_loader import AgentConfig
from shared.shared_data_models import (
    TaskDefinition, TaskResult, TaskStatus, TaskPriority,
    OptimizationRule, RuleCategory, SystemEvent, EventType,
    ComponentHealth, HealthStatus
)

# 配置日志输出到文件以便分析
LOG_FILE = "/data/user/work/production_test.log"


class ProductionRealRunTest:
    """生产级真实运行测试"""

    def __init__(self):
        self.errors = []
        self.warnings = []
        self.log_lines = []

    def test_production_run_with_chaos(self):
        """
        生产级真实运行测试：
        1. 使用 EvolutionAgent 主入口初始化完整系统
        2. 注入多层混乱（故障、延迟、并发竞争）
        3. 运行200个周期
        4. 分析日志发现BUG
        """
        print("\n" + "=" * 70)
        print("  第7轮：生产级真实运行测试")
        print("=" * 70)

        # 配置
        config = AgentConfig({
            "execution": {
                "max_concurrent_tasks": 5,
                "task_timeout_seconds": 30.0,
                "task_queue_max_size": 500,
                "cycle_cooldown_seconds": 0.0,
                "max_retries_per_task": 2,
            },
            "guardian": {
                "health_check_interval_seconds": 1.0,
                "max_component_errors_before_restart": 10,
                "auto_restart_enabled": True,
            },
            "storage": {
                "metrics_retention_cycles": 50,
                "persist_interval_cycles": 50,
            },
            "optimization": {
                "enabled": True,
                "max_active_rules": 100,
            },
            "logging": {
                "console_enabled": False,
                "file_enabled": True,
                "file_path": LOG_FILE,
                "log_level": "DEBUG",
            },
        })

        # 创建Agent
        agent = EvolutionAgent(config)
        agent.initialize()

        # 注册示例处理器
        from examples.example_task_definitions import register_all_sample_handlers
        register_all_sample_handlers(agent._execution_engine.handler_registry)

        # 注册混乱处理器
        self._register_chaos_handlers(agent)

        # 注入混乱任务
        original_feed = agent._feed_tasks

        def chaos_feed(cycle, count):
            from examples.example_task_definitions import create_sample_task_batch
            tasks = create_sample_task_batch(count)

            # 每10个周期注入故障任务
            if cycle % 10 == 0:
                tasks.append(TaskDefinition(
                    name="[故障注入] 随机失败",
                    handler="chaos_random_fail",
                    handler_params={"cycle": cycle},
                    priority=TaskPriority.NORMAL,
                    max_retries=1,
                ))

            # 每20个周期注入慢任务
            if cycle % 20 == 0:
                tasks.append(TaskDefinition(
                    name="[延迟注入] 慢任务",
                    handler="chaos_slow",
                    handler_params={"duration": 2.0},
                    priority=TaskPriority.LOW,
                    max_retries=0,
                ))

            # 每30个周期注入内存压力任务
            if cycle % 30 == 0:
                tasks.append(TaskDefinition(
                    name="[资源注入] 内存压力",
                    handler="chaos_memory",
                    handler_params={"size_mb": 10},
                    priority=TaskPriority.LOW,
                    max_retries=0,
                ))

            submitted = agent._execution_engine.submit_tasks(tasks)
            agent._log.debug(f"周期 #{cycle}: 已提交 {submitted} 个任务")

        agent._feed_tasks = chaos_feed

        # 启动守护
        agent._guardian.start_guarding()

        # 运行200个周期
        print(f"\n  开始运行 200 个周期...")
        start_time = time.time()
        total_completed = 0
        total_failed = 0
        total_tasks = 0
        cycle_errors = []

        for cycle in range(1, 201):
            try:
                agent._cycle_number = cycle
                agent._feed_tasks(cycle, 8)

                summary = agent._execution_engine.execute_cycle(cycle)

                total_completed += summary.completed_tasks
                total_failed += summary.failed_tasks
                total_tasks += summary.total_tasks

                # 优化分析
                if config.get("optimization.enabled", True):
                    try:
                        new_rules = agent._optimization_analyzer.analyze_cycle(cycle)
                        if new_rules:
                            agent._sync_bridge.sync_new_rules(new_rules)
                    except Exception as e:
                        cycle_errors.append(f"周期 {cycle} 分析: {e}")

                # 定期持久化
                if cycle % 50 == 0:
                    try:
                        agent._rule_repository.persist_to_file()
                        agent._data_collector.persist_to_file()
                    except Exception as e:
                        cycle_errors.append(f"周期 {cycle} 持久化: {e}")

                # 定期清理
                if cycle % 100 == 0:
                    try:
                        agent._data_collector.prune_old_data(max_cycles=50)
                    except Exception as e:
                        cycle_errors.append(f"周期 {cycle} 清理: {e}")

                # 进度报告
                if cycle % 50 == 0:
                    elapsed = time.time() - start_time
                    stats = agent._execution_engine.get_stats()
                    collector_stats = agent._data_collector.get_total_stats()
                    rule_stats = agent._rule_repository.get_rules_summary()
                    print(f"\n  周期 {cycle}/200:")
                    print(f"     任务: 完成={total_completed}, 失败={total_failed}")
                    print(f"     引擎: 累计={stats['total_executed']}, "
                          f"成功率={stats['overall_success_rate']:.1%}")
                    print(f"     规则: 活跃={rule_stats['active_rules']}, "
                          f"总={rule_stats['total_rules']}")
                    print(f"     采集: 总执行={collector_stats['total_executions']}, "
                          f"错误类型={collector_stats['error_types_count']}")
                    print(f"     耗时: {elapsed:.1f}s, 错误: {len(cycle_errors)}")

            except Exception as e:
                cycle_errors.append(f"周期 {cycle} 致命: {e}")
                traceback.print_exc()

        elapsed = time.time() - start_time

        # 停止
        agent._guardian.stop_guarding()
        agent._execution_engine.shutdown()

        # 最终统计
        success_rate = total_completed / total_tasks if total_tasks > 0 else 0

        print(f"\n{'=' * 70}")
        print(f"  最终报告")
        print(f"{'=' * 70}")
        print(f"  总周期: 200")
        print(f"  总任务: {total_tasks}")
        print(f"  成功: {total_completed} ({success_rate:.1%})")
        print(f"  失败: {total_failed}")
        print(f"  错误: {len(cycle_errors)}")
        print(f"  耗时: {elapsed:.1f}s")
        print(f"  平均每周期: {elapsed/200:.3f}s")
        print(f"{'=' * 70}")

        if cycle_errors:
            print(f"\n  错误列表:")
            for e in cycle_errors[:10]:
                print(f"    - {e}")

        # 验证
        assert total_tasks > 0, "没有执行任何任务"
        assert success_rate > 0.5, f"成功率过低: {success_rate:.1%}"
        assert len(cycle_errors) < 20, f"错误过多: {len(cycle_errors)}"

        # 分析日志文件
        self._analyze_log_file()

        print(f"\n  [PASS] 生产级测试通过！")
        return True

    def _register_chaos_handlers(self, agent):
        """注册混乱处理器"""

        def chaos_random_fail(**kwargs):
            """随机失败处理器"""
            if random.random() < 0.3:
                raise RuntimeError("模拟随机故障")
            return {"status": "survived"}

        def chaos_slow(**kwargs):
            """慢处理器"""
            duration = kwargs.get("duration", 1.0)
            time.sleep(duration)
            return {"status": "slow_done", "duration": duration}

        def chaos_memory(**kwargs):
            """内存压力处理器"""
            size_mb = kwargs.get("size_mb", 10)
            data = [0] * (size_mb * 1024 * 128)  # 约 size_mb MB
            time.sleep(0.01)
            del data
            return {"status": "memory_ok", "size_mb": size_mb}

        agent._execution_engine.handler_registry.register("chaos_random_fail", chaos_random_fail)
        agent._execution_engine.handler_registry.register("chaos_slow", chaos_slow)
        agent._execution_engine.handler_registry.register("chaos_memory", chaos_memory)

    def _analyze_log_file(self):
        """分析日志文件，发现潜在BUG"""
        if not os.path.exists(LOG_FILE):
            print(f"  [WARN] 日志文件不存在: {LOG_FILE}")
            return

        with open(LOG_FILE, "r") as f:
            lines = f.readlines()

        error_lines = [l for l in lines if "ERROR" in l]
        warning_lines = [l for l in lines if "WARNING" in l]

        print(f"\n  日志分析:")
        print(f"     总行数: {len(lines)}")
        print(f"     ERROR: {len(error_lines)}")
        print(f"     WARNING: {len(warning_lines)}")

        # 检查特定错误模式
        error_patterns = {
            "AttributeError": 0,
            "TypeError": 0,
            "KeyError": 0,
            "RuntimeError": 0,
            "deadlock": 0,
            "race": 0,
            "leak": 0,
        }

        for line in error_lines:
            for pattern in error_patterns:
                if pattern.lower() in line.lower():
                    error_patterns[pattern] += 1

        has_issues = False
        for pattern, count in error_patterns.items():
            if count > 0:
                print(f"     [!] {pattern}: {count}次")
                has_issues = True

        if not has_issues:
            print(f"     [OK] 无异常错误模式")

        # 清理日志文件
        try:
            os.remove(LOG_FILE)
        except:
            pass


if __name__ == "__main__":
    test = ProductionRealRunTest()
    try:
        test.test_production_run_with_chaos()
    except Exception as e:
        print(f"\n  [FAIL] 测试失败: {e}")
        traceback.print_exc()
        sys.exit(1)
