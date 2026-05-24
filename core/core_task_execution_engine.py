"""
核心任务执行引擎
主线执行模块：持续从任务队列中取出任务并执行，满负荷推进任务进度。
支持并发执行、超时控制、自动重试，并实时将执行数据上报给采集器。
"""

from __future__ import annotations

import concurrent.futures
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from shared.shared_data_models import (
    CycleSummary,
    EventType,
    SystemEvent,
    TaskDefinition,
    TaskPriority,
    TaskResult,
    TaskStatus,
)
from execution.execution_data_collector import ExecutionDataCollector
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from shared.unified_logging_service import AgentLoggingService


class TaskHandlerRegistry:
    """任务处理器注册表：管理所有可执行的任务处理函数"""

    def __init__(self):
        self._handlers: Dict[str, Callable] = {}
        self._lock = threading.Lock()

    def register(self, name: str, handler: Callable) -> None:
        """注册任务处理器"""
        with self._lock:
            self._handlers[name] = handler

    def get(self, name: str) -> Optional[Callable]:
        """获取任务处理器"""
        with self._lock:
            return self._handlers.get(name)

    def list_handlers(self) -> List[str]:
        """列出所有已注册的处理器名称"""
        with self._lock:
            return list(self._handlers.keys())


class CoreTaskExecutionEngine:
    """
    核心任务执行引擎
    - 从任务队列中持续取出任务并执行
    - 支持优先级排序、并发执行、超时控制
    - 自动重试失败任务
    - 实时上报执行数据
    """

    def __init__(
        self,
        config: Dict[str, Any],
        logging_service: AgentLoggingService,
        data_collector: ExecutionDataCollector,
        recovery_handler: ExceptionRecoveryHandler,
    ):
        self._config = config
        self._log = logging_service.get_logger("execution_engine")
        self._logging_svc = logging_service
        self._collector = data_collector
        self._recovery = recovery_handler

        # 执行状态
        self._current_cycle: int = 0
        self._is_running: bool = False
        self._total_executed: int = 0
        self._total_success: int = 0
        self._active_rules_snapshot: List[Dict[str, Any]] = []
        self._rules_lock = threading.Lock()  # 保护 _active_rules_snapshot 的并发访问

        # 并发控制
        self._max_concurrent = config.get("max_concurrent_tasks", 5)
        self._task_timeout = config.get("task_timeout_seconds", 300.0)
        self._max_retries = config.get("max_retries_per_task", 3)
        self._retry_delay = config.get("retry_delay_seconds", 2.0)
        self._queue_max_size = config.get("task_queue_max_size", 1000)

        # 任务队列（线程安全队列，必须在_queue_max_size之后创建）
        self._task_queue: queue.Queue = queue.Queue(maxsize=self._queue_max_size)

        # 处理器注册表
        self.handler_registry = TaskHandlerRegistry()

        # 线程池
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._max_concurrent,
            thread_name_prefix="task_worker",
        )

    # ── 任务管理 ──

    def submit_task(self, task: TaskDefinition) -> bool:
        """提交任务到队列（线程安全，非阻塞）"""
        try:
            self._task_queue.put_nowait(task)
            priority_str = task.priority.value if hasattr(task.priority, 'value') else str(task.priority)
            self._log.debug(f"任务入队: {task.name} (priority={priority_str})")
            return True
        except queue.Full:
            self._log.warning(f"任务队列已满({self._queue_max_size})，拒绝任务: {task.name}")
            return False

    def submit_tasks(self, tasks: List[TaskDefinition]) -> int:
        """批量提交任务"""
        count = 0
        for task in tasks:
            if self.submit_task(task):
                count += 1
        return count

    def queue_size(self) -> int:
        """获取队列中待执行任务数"""
        return self._task_queue.qsize()

    def clear_queue(self) -> int:
        """清空任务队列"""
        count = 0
        while not self._task_queue.empty():
            try:
                self._task_queue.get_nowait()
                count += 1
            except queue.Empty:
                break
        return count

    # ── 执行控制 ──

    def execute_cycle(self, cycle_number: int) -> CycleSummary:
        """
        执行一个完整周期：处理队列中所有任务。
        这是主循环的核心调用。
        """
        self._current_cycle = cycle_number
        self._is_running = True

        summary = CycleSummary(cycle_number=cycle_number)
        self._log.info(f"═══ 执行周期 {cycle_number} 开始 ═══")

        self._logging_svc.emit_event(SystemEvent(
            event_type=EventType.CYCLE_STARTED,
            source="execution_engine",
            data={"cycle_number": cycle_number},
            cycle_number=cycle_number,
        ))

        try:
            # 取出当前周期所有任务
            tasks_to_run = self._drain_queue()

            if not tasks_to_run:
                self._log.debug(f"周期 {cycle_number}: 队列为空，快速跳过")
                summary.finished_at = time.time()
                return summary

            # 按优先级排序
            priority_order = {
                TaskPriority.CRITICAL: 0,
                TaskPriority.HIGH: 1,
                TaskPriority.NORMAL: 2,
                TaskPriority.LOW: 3,
            }
            tasks_to_run.sort(key=lambda t: priority_order.get(t.priority, 2))

            summary.total_tasks = len(tasks_to_run)

            # 并发执行任务
            futures: Dict[concurrent.futures.Future, TaskDefinition] = {}
            for task in tasks_to_run:
                future = self._executor.submit(self._execute_single_task, task, cycle_number)
                futures[future] = task

            # 等待所有任务完成（使用 try/except 保护 as_completed 超时）
            try:
                for future in concurrent.futures.as_completed(futures, timeout=self._task_timeout * 2 * max(1, len(tasks_to_run) // self._max_concurrent)):
                    task = futures[future]
                    try:
                        result = future.result(timeout=self._task_timeout)
                        if result.success:
                            summary.completed_tasks += 1
                            self._total_success += 1
                        else:
                            summary.failed_tasks += 1
                            summary.total_errors += 1
                            # result.error 现在是字符串类型
                            if result.error:
                                error_type = "TaskFailed"
                                summary.error_types[error_type] = summary.error_types.get(error_type, 0) + 1
                    except concurrent.futures.TimeoutError:
                        summary.failed_tasks += 1
                        summary.total_errors += 1
                        summary.error_types["TimeoutError"] = summary.error_types.get("TimeoutError", 0) + 1
                        self._log.error(f"任务超时: {task.name}")
                    except Exception as e:
                        summary.failed_tasks += 1
                        summary.total_errors += 1
                        error_type = type(e).__name__
                        summary.error_types[error_type] = summary.error_types.get(error_type, 0) + 1
                        self._recovery.handle_exception("execution_engine", e,
                                                         context={"task": task.name})
            except concurrent.futures.TimeoutError:
                # as_completed 整体超时，统计未完成的任务为失败
                remaining = sum(1 for f in futures if not f.done())
                summary.failed_tasks += remaining
                summary.total_errors += remaining
                summary.error_types["BatchTimeout"] = summary.error_types.get("BatchTimeout", 0) + remaining
                self._log.error(f"周期 {cycle_number}: 批量执行超时，{remaining} 个任务未完成")

            # 计算统计
            durations = [r.duration_seconds for r in self._collector.get_recent_metrics(len(tasks_to_run))]
            summary.avg_duration = sum(durations) / len(durations) if durations else 0.0
            summary.finished_at = time.time()
            self._total_executed += summary.total_tasks

            self._log.info(
                f"═══ 执行周期 {cycle_number} 完成 ═══ "
                f"总计={summary.total_tasks}, 成功={summary.completed_tasks}, "
                f"失败={summary.failed_tasks}, 成功率={summary.success_rate:.1%}, "
                f"平均耗时={summary.avg_duration:.3f}s"
            )

            self._logging_svc.emit_event(SystemEvent(
                event_type=EventType.CYCLE_COMPLETED,
                source="execution_engine",
                data={"cycle_summary": summary},
                cycle_number=cycle_number,
            ))

        except Exception as e:
            self._log.error(f"执行周期 {cycle_number} 发生未预期异常: {e}")
            summary.finished_at = time.time()
            self._recovery.handle_exception("execution_engine", e,
                                             context={"cycle": cycle_number})
        finally:
            self._is_running = False
        return summary

    def apply_optimization_rules(self, rules: List[Dict[str, Any]]) -> None:
        """接收并应用优化规则（由同步桥接器调用）"""
        with self._rules_lock:
            self._active_rules_snapshot = list(rules)  # 创建副本，避免外部修改
        self._log.info(f"已加载 {len(rules)} 条优化规则到执行引擎")

    def get_stats(self) -> Dict[str, Any]:
        """获取引擎统计"""
        with self._rules_lock:
            active_rules = len(self._active_rules_snapshot)
        return {
            "current_cycle": self._current_cycle,
            "is_running": self._is_running,
            "total_executed": self._total_executed,
            "total_success": self._total_success,
            "overall_success_rate": (
                self._total_success / self._total_executed
                if self._total_executed > 0 else 0.0
            ),
            "queue_size": self.queue_size(),
            "active_rules_count": active_rules,
            "registered_handlers": self.handler_registry.list_handlers(),
        }

    def shutdown(self) -> None:
        """优雅关闭"""
        self._is_running = False
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._log.info("执行引擎已关闭")

    # ── 内部方法 ──

    def _drain_queue(self) -> List[TaskDefinition]:
        """取出队列中所有任务（线程安全）"""
        tasks = []
        while True:
            try:
                tasks.append(self._task_queue.get_nowait())
            except queue.Empty:
                break
        return tasks

    def _execute_single_task(self, task: TaskDefinition, cycle_number: int) -> TaskResult:
        """执行单个任务（含重试逻辑）"""
        result = TaskResult(task_id=task.task_id)
        handler = self.handler_registry.get(task.handler or "")

        if handler is None:
            result.status = TaskStatus.SKIPPED
            result.error = f"未找到处理器: {task.handler}"
            self._log.warning(f"跳过任务 {task.name}: 未注册处理器 '{task.handler}'")
            self._collector.record_task_execution(result, cycle_number)
            return result

        result.started_at = time.time()

        max_retries = max(0, task.max_retries)

        for attempt in range(max_retries + 1):
            try:
                result.status = TaskStatus.RUNNING
                if attempt > 0:
                    result.retry_count = attempt
                    result.status = TaskStatus.RETRYING
                    self._log.info(f"重试任务 {task.name} (第{attempt}次)")
                    time.sleep(self._retry_delay * attempt)  # 递增延迟

                self._logging_svc.emit_event(SystemEvent(
                    event_type=EventType.TASK_STARTED if attempt == 0 else EventType.TASK_RETRIED,
                    source="execution_engine",
                    data={
                        "task_id": task.task_id,
                        "task_name": task.name,
                        "attempt": attempt + 1,
                    },
                    cycle_number=cycle_number,
                ))

                # 应用优化规则预处理
                params = self._apply_rules_preprocessing(task, result)

                # 执行任务
                output = handler(**params)
                result.output = output
                result.status = TaskStatus.COMPLETED
                result.finished_at = time.time()
                result.duration_seconds = result.duration

                # 不再每次成功后清零错误计数，让守护管理器基于自己的
                # consecutive_failures 判断是否需要重启
                self._collector.record_task_execution(result, cycle_number)

                self._logging_svc.emit_event(SystemEvent(
                    event_type=EventType.TASK_COMPLETED,
                    source="execution_engine",
                    data={
                        "task_id": task.task_id,
                        "task_name": task.name,
                        "duration": result.duration,
                        "attempt": attempt + 1,
                    },
                    cycle_number=cycle_number,
                ))
                return result

            except Exception as e:
                result.error = str(e)
                result.finished_at = time.time()
                result.duration_seconds = result.duration

                if attempt == max_retries:
                    result.status = TaskStatus.FAILED
                    self._log.error(f"任务 {task.name} 最终失败: {e}")
                    self._recovery.handle_exception(
                        "execution_engine", e,
                        context={"task": task.name, "attempts": attempt + 1}
                    )

                    self._logging_svc.emit_event(SystemEvent(
                        event_type=EventType.TASK_FAILED,
                        source="execution_engine",
                        data={
                            "task_id": task.task_id,
                            "task_name": task.name,
                            "error": str(e)[:200],
                            "attempts": attempt + 1,
                        },
                        cycle_number=cycle_number,
                    ))

        self._collector.record_task_execution(result, cycle_number)
        return result

    def _apply_rules_preprocessing(self, task: TaskDefinition,
                                    result: TaskResult) -> Dict[str, Any]:
        """根据活跃的优化规则对任务参数进行预处理"""
        internal_keys = {'_optimized_timeout', '_parallel_hint'}
        params = dict(task.handler_params) if task.handler_params else {}
        with self._rules_lock:
            rules_snapshot = list(self._active_rules_snapshot)
        for rule in rules_snapshot:
            try:
                # 规则可以修改任务参数
                action = rule.get("action", "")
                if "timeout" in action.lower() and "reduce" in action.lower():
                    params["_optimized_timeout"] = True
                if "parallel" in action.lower():
                    params["_parallel_hint"] = True
            except Exception as e:
                self._log.warning(f"规则预处理异常（规则: {rule.get('name', 'unknown')}）: {e}")
        params = {k: v for k, v in params.items() if k not in internal_keys}
        return params
