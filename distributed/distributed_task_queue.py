"""
分布式任务队列 - 基于Redis的实现
支持多节点任务分发、分布式锁、任务认领机制
"""
import json
import time
import uuid
import threading
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, asdict

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

@dataclass
class DistributedTask:
    """分布式任务定义"""
    task_id: str
    name: str
    handler: str
    handler_params: Dict[str, Any]
    priority: int = 10
    max_retries: int = 3
    timeout_seconds: float = 300.0
    created_at: float = 0.0
    claimed_by: Optional[str] = None
    claimed_at: Optional[float] = None
    status: str = "pending"  # pending, claimed, completed, failed
    result: Optional[Dict] = None
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at == 0.0:
            self.created_at = time.time()


class DistributedTaskQueue:
    """
    分布式任务队列
    
    使用Redis实现：
    - 任务队列：Redis List (LPUSH/BRPOP)
    - 任务状态：Redis Hash (HSET/HGET)
    - 分布式锁：Redis SET NX EX
    - 节点心跳：Redis String with TTL
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0", 
                 node_id: Optional[str] = None):
        if not REDIS_AVAILABLE:
            raise ImportError("redis package required. Install: pip install redis")
        
        self._redis = redis.from_url(redis_url, decode_responses=True)
        self._node_id = node_id or str(uuid.uuid4())[:8]
        self._running = False
        self._heartbeat_thread: Optional[threading.Thread] = None
        
        # Redis键前缀
        self._key_prefix = "evolution:agent"
        self._queue_key = f"{self._key_prefix}:queue"
        self._task_hash_key = f"{self._key_prefix}:tasks"
        self._lock_key = f"{self._key_prefix}:lock"
        self._heartbeat_key = f"{self._key_prefix}:nodes"
        
    def submit_task(self, task: DistributedTask) -> bool:
        """提交任务到分布式队列"""
        try:
            # 存储任务详情
            task_data = asdict(task)
            self._redis.hset(self._task_hash_key, task.task_id, json.dumps(task_data))
            
            # 按优先级放入队列（使用sorted set，score为优先级）
            self._redis.zadd(self._queue_key, {task.task_id: task.priority})
            return True
        except Exception as e:
            print(f"[DistributedQueue] 提交任务失败: {e}")
            return False
    
    def claim_task(self, timeout: float = 5.0) -> Optional[DistributedTask]:
        """
        认领任务（带分布式锁）
        
        使用Redis分布式锁确保只有一个节点能认领任务
        """
        try:
            # 使用BLPOP等待任务（阻塞式）
            # 实际上使用sorted set，需要轮询
            task_ids = self._redis.zrange(self._queue_key, 0, 0)
            if not task_ids:
                return None
            
            task_id = task_ids[0]
            
            # 尝试获取分布式锁
            lock_key = f"{self._lock_key}:{task_id}"
            lock_acquired = self._redis.set(
                lock_key, self._node_id, 
                nx=True, ex=int(timeout)
            )
            
            if not lock_acquired:
                return None  # 其他节点已认领
            
            # 从队列移除
            self._redis.zrem(self._queue_key, task_id)
            
            # 获取任务详情
            task_data = self._redis.hget(self._task_hash_key, task_id)
            if not task_data:
                self._redis.delete(lock_key)
                return None
            
            task_dict = json.loads(task_data)
            task = DistributedTask(**task_dict)
            
            # 更新任务状态
            task.status = "claimed"
            task.claimed_by = self._node_id
            task.claimed_at = time.time()
            
            self._redis.hset(self._task_hash_key, task_id, json.dumps(asdict(task)))
            
            return task
            
        except Exception as e:
            print(f"[DistributedQueue] 认领任务失败: {e}")
            return None
    
    def complete_task(self, task_id: str, result: Dict[str, Any]) -> bool:
        """标记任务完成"""
        try:
            task_data = self._redis.hget(self._task_hash_key, task_id)
            if not task_data:
                return False
            
            task_dict = json.loads(task_data)
            task_dict["status"] = "completed"
            task_dict["result"] = result
            
            self._redis.hset(self._task_hash_key, task_id, json.dumps(task_dict))
            
            # 释放锁
            self._redis.delete(f"{self._lock_key}:{task_id}")
            return True
        except Exception as e:
            print(f"[DistributedQueue] 完成任务失败: {e}")
            return False
    
    def fail_task(self, task_id: str, error: str, retry: bool = True) -> bool:
        """标记任务失败，可选择重试"""
        try:
            task_data = self._redis.hget(self._task_hash_key, task_id)
            if not task_data:
                return False
            
            task_dict = json.loads(task_data)
            task_dict["status"] = "failed"
            task_dict["error"] = error
            
            if retry and task_dict.get("max_retries", 0) > 0:
                # 重试：放回队列
                task_dict["max_retries"] -= 1
                task_dict["status"] = "pending"
                task_dict["claimed_by"] = None
                task_dict["claimed_at"] = None
                
                self._redis.hset(self._task_hash_key, task_id, json.dumps(task_dict))
                self._redis.zadd(self._queue_key, {task_id: task_dict["priority"]})
            else:
                # 最终失败
                self._redis.hset(self._task_hash_key, task_id, json.dumps(task_dict))
            
            # 释放锁
            self._redis.delete(f"{self._lock_key}:{task_id}")
            return True
        except Exception as e:
            print(f"[DistributedQueue] 标记任务失败出错: {e}")
            return False
    
    def start_heartbeat(self, interval: float = 5.0):
        """启动节点心跳"""
        self._running = True
        
        def heartbeat_loop():
            while self._running:
                try:
                    self._redis.set(
                        f"{self._heartbeat_key}:{self._node_id}",
                        json.dumps({"timestamp": time.time(), "status": "alive"}),
                        ex=int(interval * 3)
                    )
                except Exception as e:
                    print(f"[DistributedQueue] 心跳失败: {e}")
                time.sleep(interval)
        
        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
    
    def stop_heartbeat(self):
        """停止节点心跳"""
        self._running = False
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2.0)
        self._redis.delete(f"{self._heartbeat_key}:{self._node_id}")
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """获取队列统计"""
        try:
            pending = self._redis.zcard(self._queue_key)
            all_tasks = self._redis.hlen(self._task_hash_key)
            nodes = len(self._redis.keys(f"{self._heartbeat_key}:*"))
            
            return {
                "pending_tasks": pending,
                "total_tasks": all_tasks,
                "active_nodes": nodes,
                "node_id": self._node_id,
            }
        except Exception as e:
            return {"error": str(e)}
    
    def cleanup_stale_tasks(self, stale_timeout: float = 300.0) -> int:
        """
        清理僵死任务（被认领但长时间未完成的）
        
        返回清理的任务数
        """
        cleaned = 0
        try:
            all_tasks = self._redis.hgetall(self._task_hash_key)
            for task_id, task_data in all_tasks.items():
                task_dict = json.loads(task_data)
                
                if task_dict.get("status") == "claimed":
                    claimed_at = task_dict.get("claimed_at", 0)
                    if time.time() - claimed_at > stale_timeout:
                        # 任务僵死，重置为pending
                        task_dict["status"] = "pending"
                        task_dict["claimed_by"] = None
                        task_dict["claimed_at"] = None
                        
                        self._redis.hset(self._task_hash_key, task_id, json.dumps(task_dict))
                        self._redis.zadd(self._queue_key, {task_id: task_dict["priority"]})
                        cleaned += 1
            
            return cleaned
        except Exception as e:
            print(f"[DistributedQueue] 清理僵死任务失败: {e}")
            return 0
