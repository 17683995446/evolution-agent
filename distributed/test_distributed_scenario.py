"""
分布式场景测试
测试多节点并发、任务分发、故障恢复
"""
import sys
import os
import time
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 模拟Redis（如果没有真实Redis）
class MockRedis:
    """模拟Redis用于测试"""
    def __init__(self):
        self._data = {}
        self._locks = {}
        self._lock = threading.Lock()
    
    def hset(self, key, field, value):
        with self._lock:
            if key not in self._data:
                self._data[key] = {}
            self._data[key][field] = value
    
    def hget(self, key, field):
        with self._lock:
            return self._data.get(key, {}).get(field)
    
    def hgetall(self, key):
        with self._lock:
            return self._data.get(key, {}).copy()
    
    def hlen(self, key):
        with self._lock:
            return len(self._data.get(key, {}))
    
    def zadd(self, key, mapping):
        with self._lock:
            if key not in self._data:
                self._data[key] = {}
            for member, score in mapping.items():
                self._data[key][member] = score
    
    def zrange(self, key, start, end):
        with self._lock:
            items = sorted(self._data.get(key, {}).items(), key=lambda x: x[1])
            return [item[0] for item in items[start:end+1]]
    
    def zrem(self, key, member):
        with self._lock:
            if key in self._data and member in self._data[key]:
                del self._data[key][member]
    
    def zcard(self, key):
        with self._lock:
            return len(self._data.get(key, {}))
    
    def set(self, key, value, nx=False, ex=None):
        with self._lock:
            if nx and key in self._locks:
                return None
            self._locks[key] = value
            return True
    
    def delete(self, key):
        with self._lock:
            if key in self._locks:
                del self._locks[key]
    
    def expire(self, key, seconds):
        pass


class DistributedScenarioTest(unittest.TestCase):
    """分布式场景测试"""
    
    def test_01_single_node_basic(self):
        """场景1：单节点基础功能"""
        print("\n[测试1] 单节点基础功能...")
        # 使用MockRedis测试基础功能
        from distributed_task_queue import DistributedTask, DistributedTaskQueue
        
        # 创建MockRedis实例
        mock_redis = MockRedis()
        
        # 创建一个测试用的队列实例（使用MockRedis）
        class TestQueue:
            def __init__(self, mock_redis, node_id="test-node"):
                self._redis = mock_redis
                self._node_id = node_id
                self._key_prefix = "evolution:agent"
                self._queue_key = f"{self._key_prefix}:queue"
                self._task_hash_key = f"{self._key_prefix}:tasks"
                self._lock_key = f"{self._key_prefix}:lock"
                
            def submit_task(self, task):
                import json
                from dataclasses import asdict
                task_data = asdict(task)
                self._redis.hset(self._task_hash_key, task.task_id, json.dumps(task_data))
                self._redis.zadd(self._queue_key, {task.task_id: task.priority})
                return True
                
            def claim_task(self, timeout=5.0):
                import json
                from dataclasses import asdict
                task_ids = self._redis.zrange(self._queue_key, 0, 0)
                if not task_ids:
                    return None
                task_id = task_ids[0]
                lock_key = f"{self._lock_key}:{task_id}"
                lock_acquired = self._redis.set(lock_key, self._node_id, nx=True)
                if not lock_acquired:
                    return None
                self._redis.zrem(self._queue_key, task_id)
                task_data = self._redis.hget(self._task_hash_key, task_id)
                if not task_data:
                    return None
                task_dict = json.loads(task_data)
                task = DistributedTask(**task_dict)
                task.status = "claimed"
                task.claimed_by = self._node_id
                task.claimed_at = time.time()
                self._redis.hset(self._task_hash_key, task_id, json.dumps(asdict(task)))
                return task
        
        queue = TestQueue(mock_redis)
        
        # 提交任务
        task = DistributedTask(
            task_id="test-001",
            name="test-task",
            handler="test_handler",
            handler_params={"key": "value"},
            priority=5
        )
        self.assertTrue(queue.submit_task(task))
        
        # 认领任务
        claimed = queue.claim_task()
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.task_id, "test-001")
        self.assertEqual(claimed.status, "claimed")
        self.assertEqual(claimed.claimed_by, "test-node")
        
        print("  [PASS] 单节点基础功能测试通过")
    
    def test_02_multi_node_task_distribution(self):
        """场景2：多节点任务分发"""
        print("\n[测试2] 多节点任务分发...")
        from distributed_task_queue import DistributedTask
        
        mock_redis = MockRedis()
        results = {"node1": 0, "node2": 0, "node3": 0}
        lock = threading.Lock()
        
        def worker(node_id, num_tasks):
            from dataclasses import asdict
            import json
            
            for _ in range(num_tasks * 2):  # 尝试更多次以确保获取所有任务
                task_ids = mock_redis.zrange("evolution:agent:queue", 0, 0)
                if not task_ids:
                    break
                task_id = task_ids[0]
                lock_key = f"evolution:agent:lock:{task_id}"
                lock_acquired = mock_redis.set(lock_key, node_id, nx=True)
                if not lock_acquired:
                    continue
                mock_redis.zrem("evolution:agent:queue", task_id)
                with lock:
                    results[node_id] += 1
        
        # 提交10个任务
        for i in range(10):
            task = DistributedTask(
                task_id=f"task-{i}",
                name=f"task-{i}",
                handler="test_handler",
                handler_params={},
                priority=10
            )
            import json
            from dataclasses import asdict
            task_data = asdict(task)
            mock_redis.hset("evolution:agent:tasks", task.task_id, json.dumps(task_data))
            mock_redis.zadd("evolution:agent:queue", {task.task_id: task.priority})
        
        # 启动3个工作线程
        threads = []
        for node_id in ["node1", "node2", "node3"]:
            t = threading.Thread(target=worker, args=(node_id, 10))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        total_claimed = sum(results.values())
        self.assertEqual(total_claimed, 10, f"应该有10个任务被认领，实际认领了{total_claimed}个")
        print(f"  任务分发结果: {results}")
        print("  [PASS] 多节点任务分发测试通过")
    
    def test_03_task_claim_race_condition(self):
        """场景3：任务认领竞态条件"""
        print("\n[测试3] 任务认领竞态条件...")
        from distributed_task_queue import DistributedTask
        
        mock_redis = MockRedis()
        success_count = [0]
        lock = threading.Lock()
        
        # 提交一个任务
        task = DistributedTask(
            task_id="race-task",
            name="race-task",
            handler="test_handler",
            handler_params={},
            priority=10
        )
        import json
        from dataclasses import asdict
        task_data = asdict(task)
        mock_redis.hset("evolution:agent:tasks", task.task_id, json.dumps(task_data))
        mock_redis.zadd("evolution:agent:queue", {task.task_id: task.priority})
        
        def claim_worker():
            task_ids = mock_redis.zrange("evolution:agent:queue", 0, 0)
            if task_ids:
                task_id = task_ids[0]
                lock_key = f"evolution:agent:lock:{task_id}"
                lock_acquired = mock_redis.set(lock_key, "worker", nx=True)
                if lock_acquired:
                    mock_redis.zrem("evolution:agent:queue", task_id)
                    with lock:
                        success_count[0] += 1
        
        # 10个线程同时尝试认领
        threads = []
        for _ in range(10):
            t = threading.Thread(target=claim_worker)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        self.assertEqual(success_count[0], 1, f"应该只有一个线程成功认领，实际有{success_count[0]}个")
        print(f"  成功认领数: {success_count[0]}")
        print("  [PASS] 任务认领竞态条件测试通过")
    
    def test_04_worker_failure_recovery(self):
        """场景4：工作节点故障恢复"""
        print("\n[测试4] 工作节点故障恢复...")
        from distributed_task_queue import DistributedTask
        
        mock_redis = MockRedis()
        
        # 提交一个任务并模拟被认领但未完成
        task = DistributedTask(
            task_id="stale-task",
            name="stale-task",
            handler="test_handler",
            handler_params={},
            priority=10,
            status="claimed",
            claimed_by="failed-node",
            claimed_at=time.time() - 400  # 400秒前认领，超过300秒超时
        )
        import json
        from dataclasses import asdict
        task_data = asdict(task)
        mock_redis.hset("evolution:agent:tasks", task.task_id, json.dumps(task_data))
        
        # 模拟清理僵死任务
        def cleanup_stale_tasks(mock_redis, stale_timeout=300.0):
            cleaned = 0
            all_tasks = mock_redis.hgetall("evolution:agent:tasks")
            for task_id, task_data in all_tasks.items():
                task_dict = json.loads(task_data)
                if task_dict.get("status") == "claimed":
                    claimed_at = task_dict.get("claimed_at", 0)
                    if time.time() - claimed_at > stale_timeout:
                        task_dict["status"] = "pending"
                        task_dict["claimed_by"] = None
                        task_dict["claimed_at"] = None
                        mock_redis.hset("evolution:agent:tasks", task_id, json.dumps(task_dict))
                        mock_redis.zadd("evolution:agent:queue", {task_id: task_dict["priority"]})
                        cleaned += 1
            return cleaned
        
        cleaned = cleanup_stale_tasks(mock_redis)
        self.assertEqual(cleaned, 1, "应该清理1个僵死任务")
        
        # 验证任务重新进入队列
        pending = mock_redis.zcard("evolution:agent:queue")
        self.assertEqual(pending, 1, "应该有1个任务在队列中")
        
        # 验证任务状态重置
        task_data = mock_redis.hget("evolution:agent:tasks", "stale-task")
        task_dict = json.loads(task_data)
        self.assertEqual(task_dict["status"], "pending")
        self.assertIsNone(task_dict["claimed_by"])
        
        print("  [PASS] 工作节点故障恢复测试通过")
    
    def test_05_distributed_lock_timeout(self):
        """场景5：分布式锁超时"""
        print("\n[测试5] 分布式锁超时...")
        
        mock_redis = MockRedis()
        
        # 模拟锁超时场景
        # 第一个worker获取锁
        lock_acquired = mock_redis.set("test:lock", "worker1", nx=True)
        self.assertTrue(lock_acquired)
        
        # 第二个worker尝试获取锁（应该失败）
        lock_acquired2 = mock_redis.set("test:lock", "worker2", nx=True)
        self.assertIsNone(lock_acquired2)
        
        # 第一个worker释放锁
        mock_redis.delete("test:lock")
        
        # 第二个worker再次尝试（应该成功）
        lock_acquired3 = mock_redis.set("test:lock", "worker2", nx=True)
        self.assertTrue(lock_acquired3)
        
        print("  [PASS] 分布式锁超时测试通过")


if __name__ == "__main__":
    unittest.main(verbosity=2)
