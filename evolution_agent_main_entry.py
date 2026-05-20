"""
持续进化Agent主入口
组装所有模块，启动永久循环：执行+优化双并行运转。
"""

from __future__ import annotations

import signal
import sys
import time
from pathlib import Path

# 确保项目根目录在Python路径中
sys.path.insert(0, str(Path(__file__).parent))

from shared.global_configuration_loader import AgentConfig
from shared.shared_data_models import (
    ComponentHealth,
    EventType,
    HealthStatus,
    SystemEvent,
)
from shared.unified_logging_service import AgentLoggingService
from execution.execution_data_collector import ExecutionDataCollector
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from execution.optimization_rule_repository import OptimizationRuleRepository
from core.core_task_execution_engine import CoreTaskExecutionEngine
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


class EvolutionAgent:
    """
    持续进化Agent
    ─────────────────────────────────────────
    启动即全速运转，执行与优化双并行，永久循环不停歇。

    四层架构:
      1. 常驻守护层 - 全程保活，异常自修复
      2. 任务执行层 - 核心干活，持续输出
      3. 智能优化层 - 实时复盘，持续进化
      4. 同步落地层 - 规则即时生效，无缝衔接
    """

    def __init__(self, config: AgentConfig):
        self._config = config
        self._log = None  # 延迟初始化
        self._is_running = False
        self._cycle_number = 0

        # 模块实例（延迟初始化）
        self._logging_service: AgentLoggingService = None  # type: ignore
        self._data_collector: ExecutionDataCollector = None  # type: ignore
        self._rule_repository: OptimizationRuleRepository = None  # type: ignore
        self._recovery_handler: ExceptionRecoveryHandler = None  # type: ignore
        self._execution_engine: CoreTaskExecutionEngine = None  # type: ignore
        self._optimization_analyzer: IntelligentOptimizationAnalyzer = None  # type: ignore
        self._sync_bridge: RuleSyncBridge = None  # type: ignore
        self._guardian: ResidentGuardianManager = None  # type: ignore

    # ── 初始化 ──

    def initialize(self) -> "EvolutionAgent":
        """初始化所有模块"""
        print("\n" + "=" * 60)
        print("  🚀 持续进化Agent - 初始化中...")
        print("=" * 60)

        # 1. 日志服务
        self._logging_service = AgentLoggingService(self._config.section("logging"))
        self._log = self._logging_service.get_logger("agent_main")
        self._log.info("日志服务已启动")

        # 2. 异常恢复处理器
        self._recovery_handler = ExceptionRecoveryHandler(
            self._config.section("guardian"), self._logging_service
        )
        self._log.info("异常恢复处理器已初始化")

        # 3. 执行数据采集器
        self._data_collector = ExecutionDataCollector(
            self._config.section("storage"), self._logging_service
        )
        self._log.info("执行数据采集器已初始化")

        # 4. 优化规则仓库
        self._rule_repository = OptimizationRuleRepository(
            self._config.section("optimization"), self._logging_service
        )
        self._log.info("优化规则仓库已初始化")

        # 5. 核心任务执行引擎
        self._execution_engine = CoreTaskExecutionEngine(
            self._config.section("execution"),
            self._logging_service,
            self._data_collector,
            self._recovery_handler,
        )
        self._log.info("核心任务执行引擎已初始化")

        # 6. 智能优化分析器
        self._optimization_analyzer = IntelligentOptimizationAnalyzer(
            self._config.section("optimization"),
            self._logging_service,
            self._data_collector,
            self._rule_repository,
            self._recovery_handler,
        )
        self._log.info("智能优化分析器已初始化")

        # 7. 规则同步桥接器
        self._sync_bridge = RuleSyncBridge(
            self._logging_service,
            self._rule_repository,
            self._execution_engine,
            self._optimization_analyzer,
        )
        self._log.info("规则同步桥接器已初始化")

        # 8. 常驻守护管理器
        self._guardian = ResidentGuardianManager(
            self._config.section("guardian"),
            self._logging_service,
            self._recovery_handler,
        )
        self._register_guardian_components()
        self._log.info("常驻守护管理器已初始化")

        print("  ✅ 所有模块初始化完成\n")
        return self

    # ── 运行控制 ──

    def start(self, max_cycles: int = 0, tasks_per_cycle: int = 5) -> None:
        """
        启动Agent永久循环。

        Args:
            max_cycles: 最大循环次数，0表示无限循环
            tasks_per_cycle: 每个周期生成的示例任务数
        """
        self._is_running = True

        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 启动守护线程
        self._guardian.start_guarding()

        # 注册示例任务处理器
        from examples.example_task_definitions import register_all_sample_handlers
        register_all_sample_handlers(self._execution_engine.handler_registry)

        self._log.info("=" * 60)
        self._log.info("  🔄 持续进化Agent - 启动运行")
        self._log.info(f"  最大循环: {'无限' if max_cycles == 0 else max_cycles}")
        self._log.info(f"  每周期任务数: {tasks_per_cycle}")
        self._log.info("=" * 60)

        try:
            self._main_loop(max_cycles, tasks_per_cycle)
        except Exception as e:
            self._log.critical(f"主循环异常退出: {e}", exc_info=True)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """优雅关闭"""
        self._is_running = False
        self._log.info("正在关闭Agent...")

        # 停止守护
        self._guardian.stop_guarding()

        # 关闭执行引擎
        self._execution_engine.shutdown()

        # 持久化数据
        try:
            self._rule_repository.persist_to_file()
            self._data_collector.persist_to_file()
            self._log.info("数据已持久化")
        except Exception as e:
            self._log.error(f"数据持久化失败: {e}")

        self._log.info("Agent已关闭")

    # ── 内部方法 ──

    def _main_loop(self, max_cycles: int, tasks_per_cycle: int) -> None:
        """
        主循环：执行+优化双并行永久循环
        ─────────────────────────────────
        while True:
            1. 生成/接收任务
            2. 全力执行当前全部任务（主线）
            3. 同步分析执行优劣（副线）
            4. 生成优化方案
            5. 执行时套用新优化逻辑
            6. 继续执行+持续复盘优化
        """
        cooldown = self._config.get("execution.cycle_cooldown_seconds", 0.1)

        while self._is_running:
            self._cycle_number += 1
            cycle = self._cycle_number

            # 检查最大循环数
            if max_cycles > 0 and cycle > max_cycles:
                self._log.info(f"已达到最大循环数 {max_cycles}，停止运行")
                break

            self._log.info(f"\n{'━' * 50}")
            self._log.info(f"  🔄 循环周期 #{cycle}")
            self._log.info(f"{'━' * 50}")

            # ── 步骤1: 生成/接收任务 ──
            self._feed_tasks(cycle, tasks_per_cycle)

            # ── 步骤2: 主线 - 全力执行当前全部任务 ──
            cycle_summary = self._execution_engine.execute_cycle(cycle)

            # ── 步骤3: 副线 - 同步分析执行优劣 ──
            if self._config.get("optimization.enabled", True):
                new_rules = self._optimization_analyzer.analyze_cycle(cycle)

                # ── 步骤4: 生成优化方案 → 步骤5: 即时同步 ──
                if new_rules:
                    self._sync_bridge.sync_new_rules(new_rules)

            # ── 步骤6: 持久化（按配置间隔） ──
            persist_interval = self._config.get("storage.persist_interval_cycles", 5)
            if cycle % persist_interval == 0:
                try:
                    self._rule_repository.persist_to_file()
                    self._data_collector.persist_to_file()
                except Exception as e:
                    self._recovery.handle_exception("agent_main", e,
                                                     context={"action": "persist"})

            # ── 打印周期报告 ──
            self._print_cycle_report(cycle, cycle_summary)

            # ── 极短冷却（接近零空闲） ──
            if cooldown > 0:
                time.sleep(cooldown)

    def _feed_tasks(self, cycle: int, count: int) -> None:
        """向执行引擎投喂任务"""
        from examples.example_task_definitions import create_sample_task_batch
        tasks = create_sample_task_batch(count)
        submitted = self._execution_engine.submit_tasks(tasks)
        self._log.debug(f"周期 #{cycle}: 已提交 {submitted} 个任务到队列")

    def _print_cycle_report(self, cycle: int, summary) -> None:
        """打印周期报告"""
        stats = self._execution_engine.get_stats()
        rule_stats = self._rule_repository.get_rules_summary()
        collector_stats = self._data_collector.get_total_stats()

        self._log.info(
            f"\n  📊 周期 #{cycle} 报告:"
            f"\n     任务: 总计={summary.total_tasks}, "
            f"成功={summary.completed_tasks}, "
            f"失败={summary.failed_tasks}, "
            f"成功率={summary.success_rate:.1%}"
            f"\n     引擎: 累计执行={stats['total_executed']}, "
            f"累计成功={stats['total_success']}, "
            f"队列={stats['queue_size']}"
            f"\n     规则: 活跃={rule_stats['active_rules']}, "
            f"总规则={rule_stats['total_rules']}, "
            f"平均效果={rule_stats['avg_effectiveness']:.2f}"
            f"\n     采集: 总执行={collector_stats['total_executions']}, "
            f"错误类型={collector_stats['error_types_count']}"
        )

    def _register_guardian_components(self) -> None:
        """注册所有需要守护的组件"""
        self._guardian.register_component(
            name="execution_engine",
            health_check=lambda: self._check_engine_health(),
            restart_action=self._restart_engine,
            critical=True,
        )
        self._guardian.register_component(
            name="optimization_analyzer",
            health_check=lambda: self._check_analyzer_health(),
            critical=True,
        )
        self._guardian.register_component(
            name="data_collector",
            health_check=lambda: self._check_collector_health(),
            critical=False,
        )
        self._guardian.register_component(
            name="rule_repository",
            health_check=lambda: self._check_repository_health(),
            critical=False,
        )

    def _check_engine_health(self) -> ComponentHealth:
        """检查执行引擎健康状态"""
        stats = self._execution_engine.get_stats()
        error_stats = self._recovery.get_error_stats()
        engine_errors = error_stats.get("components_with_errors", {}).get("execution_engine", 0)

        status = HealthStatus.HEALTHY
        if engine_errors >= 5:
            status = HealthStatus.UNHEALTHY
        elif engine_errors >= 2:
            status = HealthStatus.DEGRADED

        return ComponentHealth(
            component_name="execution_engine",
            status=status,
            error_count=engine_errors,
            metadata=stats,
        )

    def _check_analyzer_health(self) -> ComponentHealth:
        """检查优化分析器健康状态"""
        stats = self._optimization_analyzer.get_analysis_stats()
        return ComponentHealth(
            component_name="optimization_analyzer",
            status=HealthStatus.HEALTHY,
            metadata=stats,
        )

    def _check_collector_health(self) -> ComponentHealth:
        """检查数据采集器健康状态"""
        stats = self._data_collector.get_total_stats()
        return ComponentHealth(
            component_name="data_collector",
            status=HealthStatus.HEALTHY,
            metadata=stats,
        )

    def _check_repository_health(self) -> ComponentHealth:
        """检查规则仓库健康状态"""
        stats = self._rule_repository.get_rules_summary()
        return ComponentHealth(
            component_name="rule_repository",
            status=HealthStatus.HEALTHY,
            metadata=stats,
        )

    def _restart_engine(self) -> bool:
        """重启执行引擎"""
        try:
            self._execution_engine.shutdown()
            self._execution_engine = CoreTaskExecutionEngine(
                self._config.section("execution"),
                self._logging_service,
                self._data_collector,
                self._recovery_handler,
            )
            # 重新注册处理器
            from examples.example_task_definitions import register_all_sample_handlers
            register_all_sample_handlers(self._execution_engine.handler_registry)
            # 重新同步规则
            self._sync_bridge.force_resync()
            return True
        except Exception as e:
            self._log.error(f"重启执行引擎失败: {e}")
            return False

    def _signal_handler(self, signum, frame) -> None:
        """信号处理：优雅关闭"""
        sig_name = signal.Signals(signum).name
        self._log.info(f"收到信号 {sig_name}，准备关闭...")
        self._is_running = False


# ══════════════════════════════════════════════════════════════
#  命令行入口
# ══════════════════════════════════════════════════════════════

def main():
    """主入口函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="持续进化Agent - 执行与优化双并行永久循环系统"
    )
    parser.add_argument(
        "--config", "-c",
        type=str,
        default=None,
        help="配置文件路径 (YAML/JSON)",
    )
    parser.add_argument(
        "--cycles", "-n",
        type=int,
        default=0,
        help="最大循环次数 (0=无限循环, 默认: 0)",
    )
    parser.add_argument(
        "--tasks", "-t",
        type=int,
        default=5,
        help="每周期任务数 (默认: 5)",
    )
    parser.add_argument(
        "--log-level", "-l",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别 (默认: INFO)",
    )

    args = parser.parse_args()

    # 加载配置
    if args.config:
        if args.config.endswith(".json"):
            config = AgentConfig.from_json(args.config)
        else:
            config = AgentConfig.from_yaml(args.config)
    else:
        config = AgentConfig()

    # 命令行参数覆盖
    if args.log_level:
        config.set("agent.log_level", args.log_level)

    # 创建并启动Agent
    agent = EvolutionAgent(config)
    agent.initialize()
    agent.start(max_cycles=args.cycles, tasks_per_cycle=args.tasks)


if __name__ == "__main__":
    main()
