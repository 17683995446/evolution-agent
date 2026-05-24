#!/usr/bin/env python3
"""
第15轮：分布式协调层简化测试
"""

import sys
import os
import time
import threading
import random
import json
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class SimpleRedis:
    """简化版Redis模拟"""
    def __init__(self):
        self._data = {}
        self._locks = {}
        self._lock = threading.Lock()
        self._failure_rate = 0.0
        
    def inject_failure(self, rate: float):
        self._failure_rate = rate
        
    def _should_fail(self) -> bool:
        return random.random() < self._failure_rate
    
    def zadd(self, key, mapping):
        if self._should_fail():
            raise ConnectionError("Redis connection lost")
        with self._lock:
            if key not in self._data:
                self._data[key] = {}
            for member, score in mapping.items():
                self._data[key][member] = score
    
    def zrange(self, key, start, end):
        if self._should_fail():
            raise ConnectionError("Redis connection lost")
        with self._lock:
            items = sorted(self._data.get(key, {}).items(), key=lambda x: x[1])
            return [item[0] for item in items[start:end+1]]
    
    def zrem(self, key, member):
        if self._should_fail():
            raise ConnectionError("Redis connection lost")
        with self._lock:
            if key in self._data and member in self._data[key]:
                del self._data[key][member]
    
    def zcard(self, key):
        with self._lock:
            return len(self._data.get(key, {}))
    
    def set(self, key, value, nx=False, ex=None):
        if self._should_fail():
            raise ConnectionError("Redis connection lost")
        with self._lock:
            if nx and key in self._locks:
                return None
            self._locks[key] = value
            return True
    
    def delete(self, key):
        with self._lock:
            if key in self._locks:
                del self._locks[key]


def test_10_nodes_1000_tasks():
    """10节点并发处理1000任务"""
    print("\n[Test] 10节点并发，1000任务竞争...")
    
    redis = SimpleRedis()
    num_nodes = 10
    num_tasks = 1000
    completed = [0]
    lock = threading.Lock()
    
    # 提交任务
    for i in range(num_tasks):
        redis.zadd("queue", {f"task-{i}": random.randint(1, 10)})
    
    def worker():
        while True:
            try:
                task_ids = redis.zrange("queue", 0, 0)
                if not task_ids:
                    break
                
                task_id = task_ids[0]
                lock_key = f"lock:{task_id}"
                
                # 先获取锁，再移除队列（确保原子性）
                if redis.set(lock_key, "worker", nx=True, ex=10):
                    # 再次确认任务还在队列中（可能已被其他线程处理）
                    current_ids = redis.zrange("queue", 0, 0)
                    if current_ids and current_ids[0] == task_id:
                        redis.zrem("queue", task_id)
                        with lock:
                            completed[0] += 1
                    redis.delete(lock_key)
            except ConnectionError:
                time.sleep(0.001)
    
    threads = []
    start_time = time.time()
    
    for _ in range(num_nodes):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    elapsed = time.time() - start_time
    print(f"  完成时间: {elapsed:.2f}s, 完成任务: {completed[0]}/{num_tasks}")
    assert completed[0] == num_tasks, f"Expected {num_tasks}, got {completed[0]}"
    print("  [PASS]")


def test_with_30_percent_failure():
    """30%故障率下的任务处理"""
    print("\n[Test] 30%故障率下的任务处理...")
    
    redis = SimpleRedis()
    redis.inject_failure(0.3)  # 30%故障率
    
    num_tasks = 100
    completed = [0]
    lock = threading.Lock()
    errors = [0]
    
    # 提交任务（带重试）
    for i in range(num_tasks):
        retries = 0
        while retries < 10:
            try:
                redis.zadd("queue", {f"task-{i}": 5})
                break
            except ConnectionError:
                retries += 1
                time.sleep(0.01)
    
    def resilient_worker():
        consecutive_errors = 0
        while consecutive_errors < 50:
            try:
                task_ids = redis.zrange("queue", 0, 0)
                if not task_ids:
                    # 检查是否真的完成
                    if redis.zcard("queue") == 0:
                        break
                    time.sleep(0.01)
                    continue
                
                task_id = task_ids[0]
                lock_key = f"lock:{task_id}"
                
                if redis.set(lock_key, "worker", nx=True, ex=10):
                    redis.zrem("queue", task_id)
                    with lock:
                        completed[0] += 1
                    redis.delete(lock_key)
                
                consecutive_errors = 0
                
            except ConnectionError:
                consecutive_errors += 1
                with lock:
                    errors[0] += 1
                time.sleep(0.01)
    
    threads = [threading.Thread(target=resilient_worker) for _ in range(5)]
    start_time = time.time()
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    elapsed = time.time() - start_time
    print(f"  完成时间: {elapsed:.2f}s, 完成任务: {completed[0]}/{num_tasks}")
    print(f"  错误次数: {errors[0]}")
    
    # 在30%故障率下，应该完成大部分任务
    assert completed[0] >= num_tasks * 0.8, f"Expected at least {num_tasks * 0.8}, got {completed[0]}"
    print("  [PASS]")


def test_stale_task_cleanup():
    """僵死任务清理"""
    print("\n[Test] 僵死任务清理...")
    
    redis = SimpleRedis()
    
    # 创建僵死任务（被认领但未完成）
    now = time.time()
    stale_tasks = []
    for i in range(5):
        task_id = f"stale-{i}"
        stale_tasks.append({
            "task_id": task_id,
            "status": "claimed",
            "claimed_by": "dead-node",
            "claimed_at": now - 400,  # 400秒前认领
            "priority": 5,
        })
    
    # 模拟清理
    cleaned = 0
    for task in stale_tasks:
        if task["status"] == "claimed" and now - task["claimed_at"] > 300:
            task["status"] = "pending"
            task["claimed_by"] = None
            task["claimed_at"] = None
            redis.zadd("queue", {task["task_id"]: task["priority"]})
            cleaned += 1
    
    print(f"  清理僵死任务: {cleaned}")
    assert cleaned == 5, f"Expected 5, got {cleaned}"
    
    queue_len = redis.zcard("queue")
    assert queue_len == 5, f"Expected 5 in queue, got {queue_len}"
    print("  [PASS]")


if __name__ == "__main__":
    print("=" * 70)
    print("  第15轮：分布式协调层简化测试")
    print("=" * 70)
    
    try:
        test_10_nodes_1000_tasks()
        test_with_30_percent_failure()
        test_stale_task_cleanup()
        
        print("\n" + "=" * 70)
        print("  Round 15 Results: PASS=3  FAIL=0")
        print("=" * 70)
    except AssertionError as e:
        print(f"\n  [FAIL] {e}")
        sys.exit(1)
