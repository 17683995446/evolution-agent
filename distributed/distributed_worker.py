"""
分布式工作节点
从Redis队列认领并执行任务
"""
import time
import threading
from typing import Callable, Dict, Any, Optional

from .distributed_task_queue import DistributedTaskQueue, DistributedTask


class DistributedWorker:
    """分布式工作节点"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0", 
                 node_id: Optional[str] = None):
        self._queue = DistributedTaskQueue(redis_url, node_id)
        self._handlers: Dict[str, Callable] = {}
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._stats_lock = threading.Lock()
        self._stats = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_retried": 0,
        }
    
    def register_handler(self, name: str, handler: Callable):
        """注册任务处理器"""
        self._handlers[name] = handler
    
    def start(self):
        """启动工作节点"""
        self._running = True
        self._queue.start_heartbeat()
        
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        
        print(f"[Worker-{self._queue._node_id}] 工作节点已启动")
    
    def stop(self):
        """停止工作节点"""
        self._running = False
        self._queue.stop_heartbeat()
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
        print(f"[Worker-{self._queue._node_id}] 工作节点已停止")
    
    def _worker_loop(self):
        """工作循环"""
        while self._running:
            try:
                # 认领任务（阻塞式，带超时）
                task = self._queue.claim_task(timeout=10.0)
                
                if not task:
                    time.sleep(0.1)
                    continue
                
                # 执行任务
                handler = self._handlers.get(task.handler)
                if not handler:
                    self._queue.fail_task(
                        task.task_id, 
                        f"未找到处理器: {task.handler}",
                        retry=False
                    )
                    with self._stats_lock:
                        self._stats["tasks_failed"] += 1
                    continue
                
                try:
                    result = handler(**task.handler_params)
                    self._queue.complete_task(task.task_id, result)
                    with self._stats_lock:
                        self._stats["tasks_completed"] += 1
                except Exception as e:
                    error_msg = str(e)
                    retry = task.max_retries > 0
                    self._queue.fail_task(task.task_id, error_msg, retry=retry)
                    with self._stats_lock:
                        self._stats["tasks_failed"] += 1
                        if retry:
                            self._stats["tasks_retried"] += 1
                    
            except Exception as e:
                print(f"[Worker-{self._queue._node_id}] 工作循环异常: {e}")
                time.sleep(1.0)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取工作节点统计"""
        with self._stats_lock:
            stats_copy = {
                **self._stats,
                "node_id": self._queue._node_id,
                "handlers": list(self._handlers.keys()),
            }
        return stats_copy
