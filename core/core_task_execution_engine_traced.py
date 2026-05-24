"""
核心任务执行引擎 - 超详细日志追踪版本
每句代码后打印日志，用于验证执行路径
"""

from __future__ import annotations

import concurrent.futures
import queue
import threading
import time
import traceback
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
    """任务处理器注册表"""

    def __init__(self):
        self._log = AgentLoggingService({"console_enabled": True, "file_enabled": False}).get_logger("handler_registry")
        self._log.debug("[LINE 32] TaskHandlerRegistry.__init__ 开始")
        self._handlers: Dict[str, Callable] = {}
        self._log.debug("[LINE 34] self._handlers = {} 初始化完成")
        self._lock = threading.Lock()
        self._log.debug("[LINE 36] self._lock = threading.Lock() 完成")
        self._log.debug("[LINE 37] TaskHandlerRegistry.__init__ 完成")

    def register(self, name: str, handler: Callable) -> None:
        """注册任务处理器"""
        self._log.debug(f"[LINE 41] register() 开始: name={name}")
        with self._lock:
            self._log.debug(f"[LINE 43] 获取锁成功")
            self._handlers[name] = handler
            self._log.debug(f"[LINE 45] self._handlers['{name}'] = handler 完成")
        self._log.debug(f"[LINE 46] register() 完成: name={name}")

    def get(self, name: str) -> Optional[Callable]:
        """获取任务处理器"""
        self._log.debug(f"[LINE 50] get() 开始: name={name}")
        with self._lock:
            self._log.debug(f"[LINE 52] 获取锁成功")
            handler = self._handlers.get(name)
            self._log.debug(f"[LINE 54] handler = self._handlers.get('{name}') = {handler}")
        self._log.debug(f"[LINE 55] get() 完成: 返回 {handler}")
        return handler

    def list_handlers(self) -> List[str]:
        """列出所有已注册的处理器名称"""
        self._log.debug("[LINE 59] list_handlers() 开始")
        with self._lock:
            self._log.debug("[LINE 61] 获取锁成功")
            handlers = list(self._handlers.keys())
            self._log.debug(f"[LINE 63] handlers = {handlers}")
        self._log.debug(f"[LINE 64] list_handlers() 完成: 返回 {len(handlers)} 个处理器")
        return handlers


class CoreTaskExecutionEngineTraced:
    """
    核心任务执行引擎 - 超详细日志追踪版本
    """

    def __init__(
        self,
        config: Dict[str, Any],
        logging_service: AgentLoggingService,
        data_collector: ExecutionDataCollector,
        recovery_handler: ExceptionRecoveryHandler,
    ):
        self._log = logging_service.get_logger("execution_engine_traced")
        self._log.debug("[LINE 82] CoreTaskExecutionEngineTraced.__init__ 开始")
        
        self._config = config
        self._log.debug(f"[LINE 85] self._config = {config}")
        
        self._logging_svc = logging_service
        self._log.debug("[LINE 88] self._logging_svc 赋值完成")
        
        self._collector = data_collector
        self._log.debug("[LINE 91] self._collector 赋值完成")
        
        self._recovery = recovery_handler
        self._log.debug("[LINE 94] self._recovery 赋值完成")

        # 执行状态
        self._log.debug("[LINE 97] 初始化执行状态...")
        self._current_cycle: int = 0
        self._log.debug("[LINE 99] self._current_cycle = 0")
        
        self._is_running: bool = False
        self._log.debug("[LINE 102] self._is_running = False")
        
        self._total_executed: int = 0
        self._log.debug("[LINE 105] self._total_executed = 0")
        
        self._total_success: int = 0
        self._log.debug("[LINE 108] self._total_success = 0")
        
        self._active_rules_snapshot: List[Dict[str, Any]] = []
        self._log.debug("[LINE 111] self._active_rules_snapshot = []")
        
        # 任务结果缓存
        self._task_results: Dict[str, TaskResult] = {}
        self._results_lock = threading.Lock()
        self._log.debug("[LINE 115] self._task_results = {} 初始化完成")

        # 并发控制
        self._log.debug("[LINE 114] 初始化并发控制配置...")
        self._max_concurrent = config.get("max_concurrent_tasks", 5)
        self._log.debug(f"[LINE 116] self._max_concurrent = {self._max_concurrent}")
        
        self._task_timeout = config.get("task_timeout_seconds", 300.0)
        self._log.debug(f"[LINE 119] self._task_timeout = {self._task_timeout}")
        
        self._max_retries = config.get("max_retries_per_task", 3)
        self._log.debug(f"[LINE 122] self._max_retries = {self._max_retries}")
        
        self._retry_delay = config.get("retry_delay_seconds", 2.0)
        self._log.debug(f"[LINE 125] self._retry_delay = {self._retry_delay}")
        
        self._queue_max_size = config.get("task_queue_max_size", 1000)
        self._log.debug(f"[LINE 128] self._queue_max_size = {self._queue_max_size}")

        # 任务队列
        self._log.debug("[LINE 131] 初始化任务队列...")
        self._task_queue: queue.Queue = queue.Queue(maxsize=self._queue_max_size)
        self._log.debug("[LINE 133] self._task_queue = queue.Queue() 完成")

        # 处理器注册表
        self._log.debug("[LINE 136] 初始化处理器注册表...")
        self.handler_registry = TaskHandlerRegistry()
        self._log.debug("[LINE 138] self.handler_registry = TaskHandlerRegistry() 完成")

        # 线程池
        self._log.debug("[LINE 141] 初始化线程池...")
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._max_concurrent,
            thread_name_prefix="task_worker",
        )
        self._log.debug("[LINE 146] self._executor = ThreadPoolExecutor() 完成")
        
        self._log.debug("[LINE 148] CoreTaskExecutionEngineTraced.__init__ 完成")

    def submit_task(self, task: TaskDefinition) -> bool:
        """提交任务到队列"""
        self._log.debug(f"[LINE 152] submit_task() 开始: task.name={task.name}")
        try:
            self._log.debug("[LINE 154] 尝试 put_nowait...")
            self._task_queue.put_nowait(task)
            self._log.debug("[LINE 156] put_nowait 成功")
            self._log.debug(f"[LINE 157] 任务入队: {task.name}")
            return True
        except queue.Full:
            self._log.warning(f"[LINE 160] 队列已满，拒绝任务: {task.name}")
            return False

    def submit_tasks(self, tasks: List[TaskDefinition]) -> int:
        """批量提交任务"""
        self._log.debug(f"[LINE 166] submit_tasks() 开始: {len(tasks)} 个任务")
        count = 0
        for i, task in enumerate(tasks):
            self._log.debug(f"[LINE 169] 提交第 {i+1}/{len(tasks)} 个任务: {task.name}")
            if self.submit_task(task):
                count += 1
                self._log.debug(f"[LINE 172] 提交成功，当前成功数: {count}")
            else:
                self._log.debug(f"[LINE 174] 提交失败")
        self._log.debug(f"[LINE 175] submit_tasks() 完成: 成功 {count}/{len(tasks)}")
        return count

    def queue_size(self) -> int:
        """获取队列中待执行任务数"""
        self._log.debug("[LINE 180] queue_size() 开始")
        size = self._task_queue.qsize()
        self._log.debug(f"[LINE 182] queue_size() = {size}")
        return size

    def execute_cycle(self, cycle_number: int) -> CycleSummary:
        """执行一个完整周期"""
        self._log.info(f"[LINE 187] execute_cycle() 开始: cycle_number={cycle_number}")
        
        self._current_cycle = cycle_number
        self._log.debug(f"[LINE 190] self._current_cycle = {cycle_number}")
        
        self._is_running = True
        self._log.debug("[LINE 193] self._is_running = True")

        summary = CycleSummary(cycle_number=cycle_number)
        self._log.debug(f"[LINE 196] summary = CycleSummary(cycle_number={cycle_number})")

        self._log.info(f"[LINE 198] ═══ 执行周期 {cycle_number} 开始 ═══")

        # 发出周期开始事件
        self._log.debug("[LINE 201] 发出 CYCLE_STARTED 事件...")
        self._logging_svc.emit_event(SystemEvent(
            event_type=EventType.CYCLE_STARTED,
            source="execution_engine_traced",
            data={"cycle_number": cycle_number},
            cycle_number=cycle_number,
        ))
        self._log.debug("[LINE 209] CYCLE_STARTED 事件已发出")

        # 取出当前周期所有任务
        self._log.debug("[LINE 212] 调用 _drain_queue()...")
        tasks_to_run = self._drain_queue()
        self._log.debug(f"[LINE 214] _drain_queue() 返回 {len(tasks_to_run)} 个任务")

        if not tasks_to_run:
            self._log.debug("[LINE 217] 队列为空，快速跳过")
            summary.finished_at = time.time()
            self._log.debug(f"[LINE 219] summary.finished_at = {summary.finished_at}")
            self._is_running = False
            self._log.debug("[LINE 221] self._is_running = False")
            return summary

        # 按优先级排序
        self._log.debug("[LINE 225] 按优先级排序任务...")
        priority_order = {
            TaskPriority.CRITICAL: 0,
            TaskPriority.HIGH: 1,
            TaskPriority.NORMAL: 2,
            TaskPriority.LOW: 3,
        }
        tasks_to_run.sort(key=lambda t: priority_order.get(t.priority, 2))
        self._log.debug("[LINE 234] 任务排序完成")

        summary.total_tasks = len(tasks_to_run)
        self._log.debug(f"[LINE 237] summary.total_tasks = {summary.total_tasks}")

        # 并发执行任务
        self._log.debug("[LINE 240] 开始并发执行任务...")
        futures: Dict[concurrent.futures.Future, TaskDefinition] = {}
        for i, task in enumerate(tasks_to_run):
            self._log.debug(f"[LINE 243] 提交第 {i+1}/{len(tasks_to_run)} 个任务到线程池: {task.name}")
            future = self._executor.submit(self._execute_single_task, task, cycle_number)
            futures[future] = task
            self._log.debug(f"[LINE 246] future 创建完成: {future}")

        # 等待所有任务完成
        self._log.debug(f"[LINE 249] 等待 {len(futures)} 个任务完成...")
        completed_count = 0
        for future in concurrent.futures.as_completed(futures, timeout=self._task_timeout * 2):
            task = futures[future]
            completed_count += 1
            self._log.debug(f"[LINE 253] 任务完成 {completed_count}/{len(futures)}: {task.name}")
            try:
                self._log.debug("[LINE 255] 尝试获取 future.result()...")
                result = future.result(timeout=self._task_timeout)
                self._log.debug(f"[LINE 257] future.result() 成功: success={result.success}")
                if result.success:
                    summary.completed_tasks += 1
                    self._total_success += 1
                    self._log.debug(f"[LINE 261] 成功计数+1: summary.completed_tasks={summary.completed_tasks}")
                else:
                    summary.failed_tasks += 1
                    summary.total_errors += 1
                    self._log.debug(f"[LINE 265] 失败计数+1: summary.failed_tasks={summary.failed_tasks}")
                    if result.error:
                        error_type = type(result.error).__name__
                        summary.error_types[error_type] = summary.error_types.get(error_type, 0) + 1
                        self._log.debug(f"[LINE 270] 错误类型计数: {error_type}={summary.error_types[error_type]}")
            except concurrent.futures.TimeoutError:
                self._log.error(f"[LINE 273] 任务超时: {task.name}")
                summary.failed_tasks += 1
                summary.total_errors += 1
            except Exception as e:
                self._log.error(f"[LINE 277] 任务异常: {task.name}, error={e}")
                summary.failed_tasks += 1
                summary.total_errors += 1
                self._recovery.handle_exception("execution_engine_traced", e, context={"task": task.name})

        # 计算统计
        self._log.debug("[LINE 283] 计算周期统计...")
        durations = [r.duration_seconds for r in self._collector.get_recent_metrics(len(tasks_to_run))]
        self._log.debug(f"[LINE 285] 获取到 {len(durations)} 个时长记录")
        summary.avg_duration = sum(durations) / len(durations) if durations else 0.0
        self._log.debug(f"[LINE 287] summary.avg_duration = {summary.avg_duration}")
        
        summary.finished_at = time.time()
        self._log.debug(f"[LINE 290] summary.finished_at = {summary.finished_at}")
        
        self._total_executed += summary.total_tasks
        self._log.debug(f"[LINE 293] self._total_executed = {self._total_executed}")

        self._log.info(
            f"[LINE 296] ═══ 执行周期 {cycle_number} 完成 ═══ "
            f"总计={summary.total_tasks}, 成功={summary.completed_tasks}, "
            f"失败={summary.failed_tasks}, 成功率={summary.success_rate:.1%}, "
            f"平均耗时={summary.avg_duration:.3f}s"
        )

        self._log.debug("[LINE 302] 发出 CYCLE_COMPLETED 事件...")
        self._logging_svc.emit_event(SystemEvent(
            event_type=EventType.CYCLE_COMPLETED,
            source="execution_engine_traced",
            data={"cycle_summary": summary},
            cycle_number=cycle_number,
        ))
        self._log.debug("[LINE 310] CYCLE_COMPLETED 事件已发出")

        self._is_running = False
        self._log.debug("[LINE 313] self._is_running = False")
        
        self._log.debug("[LINE 315] execute_cycle() 完成")
        return summary

    def _drain_queue(self) -> List[TaskDefinition]:
        """取出队列中所有任务"""
        self._log.debug("[LINE 320] _drain_queue() 开始")
        tasks = []
        count = 0
        while True:
            try:
                task = self._task_queue.get_nowait()
                tasks.append(task)
                count += 1
                self._log.debug(f"[LINE 327] 取出第 {count} 个任务: {task.name}")
            except queue.Empty:
                self._log.debug("[LINE 329] 队列为空，停止取出")
                break
        self._log.debug(f"[LINE 331] _drain_queue() 完成: 取出 {len(tasks)} 个任务")
        return tasks

    def _execute_single_task(self, task: TaskDefinition, cycle_number: int) -> TaskResult:
        """执行单个任务"""
        self._log.debug(f"[LINE 336] _execute_single_task() 开始: task={task.name}, cycle={cycle_number}")
        
        result = TaskResult(task_id=task.task_id)
        self._log.debug(f"[LINE 339] result = TaskResult(task_id={task.task_id})")
        
        handler = self.handler_registry.get(task.handler or "")
        self._log.debug(f"[LINE 342] handler = {handler}")

        if handler is None:
            self._log.warning(f"[LINE 345] 未找到处理器: {task.handler}")
            result.status = TaskStatus.SKIPPED
            result.error = f"未找到处理器: {task.handler}"
            self._collector.record_task_execution(result, cycle_number)
            self._log.debug("[LINE 350] 记录跳过任务到采集器")
            return result

        result.started_at = time.time()
        self._log.debug(f"[LINE 354] result.started_at = {result.started_at}")

        for attempt in range(task.max_retries + 1):
            self._log.debug(f"[LINE 357] 尝试 {attempt + 1}/{task.max_retries + 1}")
            try:
                result.status = TaskStatus.RUNNING
                self._log.debug("[LINE 360] result.status = TaskStatus.RUNNING")
                
                if attempt > 0:
                    result.retry_count = attempt
                    result.status = TaskStatus.RETRYING
                    self._log.info(f"[LINE 365] 重试任务 {task.name} (第{attempt}次)")
                    delay = self._retry_delay * attempt
                    self._log.debug(f"[LINE 367] 延迟 {delay}s...")
                    time.sleep(delay)

                self._log.debug("[LINE 370] 发出 TASK_STARTED 事件...")
                self._logging_svc.emit_event(SystemEvent(
                    event_type=EventType.TASK_STARTED if attempt == 0 else EventType.TASK_RETRIED,
                    source="execution_engine_traced",
                    data={"task_id": task.task_id, "task_name": task.name, "attempt": attempt + 1},
                    cycle_number=cycle_number,
                ))
                self._log.debug("[LINE 378] TASK_STARTED 事件已发出")

                # 应用优化规则预处理
                self._log.debug("[LINE 381] 调用 _apply_rules_preprocessing()...")
                params = self._apply_rules_preprocessing(task, result)
                self._log.debug(f"[LINE 383] _apply_rules_preprocessing() 返回: {params}")

                # 执行任务
                self._log.debug(f"[LINE 386] 调用 handler: {task.handler}({params})")
                output = handler(**params)
                self._log.debug(f"[LINE 388] handler 返回: {output}")
                
                result.output = output
                self._log.debug("[LINE 391] result.output = output")
                
                result.status = TaskStatus.COMPLETED
                self._log.debug("[LINE 394] result.status = TaskStatus.COMPLETED")
                
                result.finished_at = time.time()
                self._log.debug(f"[LINE 397] result.finished_at = {result.finished_at}")
                
                result.duration_seconds = result.duration
                self._log.debug(f"[LINE 400] result.duration_seconds = {result.duration_seconds}")

                self._recovery.reset_component_errors("execution_engine_traced")
                self._log.debug("[LINE 403] 重置组件错误计数")
                
                self._collector.record_task_execution(result, cycle_number)
                self._log.debug("[LINE 406] 记录任务执行到采集器")
                
                self._store_task_result(result)
                self._log.debug("[LINE 408] 任务结果已存储")

                self._log.debug("[LINE 410] 发出 TASK_COMPLETED 事件...")
                self._logging_svc.emit_event(SystemEvent(
                    event_type=EventType.TASK_COMPLETED,
                    source="execution_engine_traced",
                    data={"task_id": task.task_id, "task_name": task.name, "duration": result.duration, "attempt": attempt + 1},
                    cycle_number=cycle_number,
                ))
                self._log.debug("[LINE 418] TASK_COMPLETED 事件已发出")
                
                self._log.debug("[LINE 420] _execute_single_task() 成功返回")
                return result

            except Exception as e:
                self._log.error(f"[LINE 424] 任务执行异常: {e}")
                result.error = str(e)
                result.finished_at = time.time()
                result.duration_seconds = result.duration

                if attempt == task.max_retries:
                    self._log.error(f"[LINE 429] 任务 {task.name} 最终失败")
                    result.status = TaskStatus.FAILED
                    self._recovery.handle_exception("execution_engine_traced", e, context={"task": task.name, "attempts": attempt + 1})

                    self._log.debug("[LINE 433] 发出 TASK_FAILED 事件...")
                    self._logging_svc.emit_event(SystemEvent(
                        event_type=EventType.TASK_FAILED,
                        source="execution_engine_traced",
                        data={"task_id": task.task_id, "task_name": task.name, "error": str(e)[:200], "attempts": attempt + 1},
                        cycle_number=cycle_number,
                    ))
                    self._log.debug("[LINE 441] TASK_FAILED 事件已发出")

        self._collector.record_task_execution(result, cycle_number)
        self._log.debug("[LINE 445] 记录失败任务到采集器")
        
        self._store_task_result(result)
        self._log.debug("[LINE 447] 失败任务结果已存储")
        
        self._log.debug("[LINE 449] _execute_single_task() 失败返回")
        return result

    def _apply_rules_preprocessing(self, task: TaskDefinition, result: TaskResult) -> Dict[str, Any]:
        """根据活跃的优化规则对任务参数进行预处理"""
        self._log.debug("[LINE 450] _apply_rules_preprocessing() 开始")
        params = dict(task.handler_params)
        self._log.debug(f"[LINE 452] 原始参数: {params}")
        
        for i, rule in enumerate(self._active_rules_snapshot):
            self._log.debug(f"[LINE 455] 处理规则 {i+1}/{len(self._active_rules_snapshot)}: {rule.get('name', 'unknown')}")
            try:
                action = rule.get("action", "")
                self._log.debug(f"[LINE 458] rule.action = {action}")
                if "timeout" in action.lower() and "reduce" in action.lower():
                    params["_optimized_timeout"] = True
                    self._log.debug("[LINE 461] 添加 _optimized_timeout = True")
                if "parallel" in action.lower():
                    params["_parallel_hint"] = True
                    self._log.debug("[LINE 464] 添加 _parallel_hint = True")
            except Exception as e:
                self._log.warning(f"[LINE 467] 规则预处理异常: {e}")
        
        self._log.debug(f"[LINE 469] _apply_rules_preprocessing() 完成: {params}")
        return params

    def start(self) -> None:
        """启动执行引擎"""
        self._log.info("[LINE 475] start() 开始")
        self._is_running = True
        self._log.debug("[LINE 477] self._is_running = True")
        self._log.info("[LINE 478] 执行引擎已启动")

    def stop(self) -> None:
        """停止执行引擎"""
        self._log.info("[LINE 482] stop() 开始")
        self.shutdown()
        self._log.info("[LINE 484] 执行引擎已停止")

    def get_task_result(self, task_id: str) -> Optional[TaskResult]:
        """获取任务结果"""
        self._log.debug(f"[LINE 489] get_task_result() 开始: task_id={task_id}")
        with self._results_lock:
            result = self._task_results.get(task_id)
        self._log.debug(f"[LINE 492] get_task_result() 完成: 返回 {result}")
        return result
    
    def _store_task_result(self, result: TaskResult) -> None:
        """存储任务结果"""
        self._log.debug(f"[LINE 496] _store_task_result() 开始: task_id={result.task_id}")
        with self._results_lock:
            self._task_results[result.task_id] = result
            # 限制缓存大小，保留最近1000个结果
            if len(self._task_results) > 1000:
                oldest_key = next(iter(self._task_results))
                del self._task_results[oldest_key]
        self._log.debug(f"[LINE 503] _store_task_result() 完成: 已存储结果")

    def shutdown(self) -> None:
        """优雅关闭"""
        self._log.info("[LINE 497] shutdown() 开始")
        self._is_running = False
        self._log.debug("[LINE 499] self._is_running = False")
        
        self._log.debug("[LINE 501] 关闭线程池...")
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._log.debug("[LINE 503] 线程池已关闭")
        
        self._log.info("[LINE 505] 执行引擎已关闭")
