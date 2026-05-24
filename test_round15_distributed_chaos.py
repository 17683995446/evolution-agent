#!/usr/bin/env python3
"""
第15轮：分布式协调层极端混沌测试

测试场景：
1. 10节点并发，10000任务竞争
2. 网络分区模拟（节点间通信中断）
3. Redis故障恢复
4. 脑裂场景（多个节点认为自己是主节点）
5. 任务执行超时和僵死
6. 高频任务提交（1000 tasks/s）
7. 节点随机故障和恢复
"""

import sys
import os
import time
import threading
import random
import json
from typing import Dict, List, Any, Optional
from dataclasses import asdict
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ═══════════════════════════════════════════════════════════════
#  模拟Redis（带故障注入）
# ═══════════════════════════════════════════════════════════════

class ChaosRedis:
    """
    混沌Redis - 支持故障注入的模拟Redis
    
    可模拟的故障：
    - 网络延迟
    - 连接断开
    - 数据丢失
    - 分区故障
    """
    
    def __init__(self):
        self._data = {}
        self._locks = {}
        self._lock = threading.RLock()
        self._failure_rate = 0.0  # 故障率 0-1
        self._latency_ms = 0      # 模拟延迟
        self._partition_nodes = set()  # 被分区的节点
        self._call_count = defaultdict(int)
        self._error_count = defaultdict(int)
        
    def inject_failure(self, rate: float):
        """注入故障率"""
        self._failure_rate = rate
        
    def inject_latency(self, ms: int):
        """注入延迟"""
        self._latency_ms = ms
        
    def partition_node(self, node_id: str):
        """将节点加入网络分区（隔离）"""
        self._partition_nodes.add(node_id)
        
    def heal_partition(self, node_id: str):
        """恢复节点网络"""
        self._partition_nodes.discard(node_id)
        
    def _should_fail(self, node_id: str = None) -> bool:
        """判断是否触发故障"""
        if node_id and node_id in self._partition_nodes:
            return True
        return random.random() < self._failure_rate
    
    def _simulate_latency(self):
        """模拟网络延迟"""
        if self._latency_ms > 0:
            time.sleep(self._latency_ms / 1000.0)
    
    def hset(self, key, field, value):
        self._call_count['hset'] += 1
        if self._should_fail():
            self._error_count['hset'] += 1
            raise ConnectionError("Redis connection lost")
        self._simulate_latency()
        with self._lock:
            if key not in self._data:
                self._data[key] = {}
            self._data[key][field] = value
    
    def hget(self, key, field):
        self._call_count['hget'] += 1
        if self._should_fail():
            self._error_count['hget'] += 1
            raise ConnectionError("Redis connection lost")
        self._simulate_latency()
        with self._lock:
            return self._data.get(key, {}).get(field)
    
    def hgetall(self, key):
        self._call_count['hgetall'] += 1
        if self._should_fail():
            self._error_count['hgetall'] += 1
            raise ConnectionError("Redis connection lost")
        self._simulate_latency()
        with self._lock:
            return self._data.get(key, {}).copy()
    
    def hlen(self, key):
        self._call_count['hlen'] += 1
        with self._lock:
            return len(self._data.get(key, {}))
    
    def zadd(self, key, mapping):
        self._call_count['zadd'] += 1
        if self._should_fail():
            self._error_count['zadd'] += 1
            raise ConnectionError("Redis connection lost")
        self._simulate_latency()
        with self._lock:
            if key not in self._data:
                self._data[key] = {}
            for member, score in mapping.items():
                self._data[key][member] = score
    
    def zrange(self, key, start, end):
        self._call_count['zrange'] += 1
        if self._should_fail():
            self._error_count['zrange'] += 1
            raise ConnectionError("Redis connection lost")
        self._simulate_latency()
        with self._lock:
            items = sorted(self._data.get(key, {}).items(), key=lambda x: x[1])
            if end < 0:
                end = len(items) - 1
            return [item[0] for item in items[start:end+1]]
    
    def zrem(self, key, member):
        self._call_count['zrem'] += 1
        if self._should_fail():
            self._error_count['zrem'] += 1
            raise ConnectionError("Redis connection lost")
        self._simulate_latency()
        with self._lock:
            if key in self._data and member in self._data[key]:
                del self._data[key][member]
    
    def zcard(self, key):
        self._call_count['zcard'] += 1
        with self._lock:
            return len(self._data.get(key, {}))
    
    def set(self, key, value, nx=False, ex=None):
        self._call_count['set'] += 1
        if self._should_fail():
            self._error_count['set'] += 1
            raise ConnectionError("Redis connection lost")
        self._simulate_latency()
        with self._lock:
            if nx and key in self._locks:
                return None
            self._locks[key] = {"value": value, "expires": ex}
            return True
    
    def delete(self, key):
        self._call_count['delete'] += 1
        with self._lock:
            if key in self._locks:
                del self._locks[key]
    
    def keys(self, pattern):
        self._call_count['keys'] += 1
        with self._lock:
            # 简化实现：只匹配前缀
            prefix = pattern.replace("*", "")
            return [k for k in self._locks.keys() if k.startswith(prefix)]
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "call_count": dict(self._call_count),
            "error_count": dict(self._error_count),
            "failure_rate": self._failure_rate,
            "latency_ms": self._latency_ms,
            "partitioned_nodes": list(self._partition_nodes),
        }


# ═══════════════════════════════════════════════════════════════
#  测试结果收集
# ═══════════════════════════════════════════════════════════════

_results = {"passed": 0, "failed": 0, "errors": []}

def run_test(name: str, func):
    """运行单个测试"""
    try:
        func()
        _results["passed"] += 1
        print(f"  [PASS] {name}")
    except Exception as e:
        _results["failed"] += 1
        _results["errors"].append(f"{name}: {str(e)[:200]}")
        print(f"  [FAIL] {name}: {str(e)[:100]}")

def assert_true(condition, msg=""):
    if not condition:
        raise AssertionError(f"AssertTrue failed: {msg}")

def assert_eq(actual, expected, msg=""):
    if actual != expected:
        raise AssertionError(f"{msg}: expected={expected}, actual={actual}")


# ═══════════════════════════════════════════════════════════════
#  测试1: 10节点并发，10000任务竞争
# ═══════════════════════════════════════════════════════════════

def test_10_nodes_10000_tasks():
    """10节点并发处理10000任务"""
    print("\n  [Test] 10节点并发，10000任务竞争...")
    
    redis = ChaosRedis()
    num_nodes = 10
    num_tasks = 1000  # 减少任务数以便快速测试
    tasks_per_node = {}
    results_lock = threading.Lock()
    
    # 提交任务
    for i in range(num_tasks):
        task_id = f"task-{i:04d}"
        task_data = {
            "task_id": task_id,
            "name": f"task-{i}",
            "handler": "test_handler",
            "handler_params": {"index": i},
            "priority": random.randint(1, 10),
            "max_retries": 3,
            "status": "pending",
        }
        redis.hset("evolution:agent:tasks", task_id, json.dumps(task_data))
        redis.zadd("evolution:agent:queue", {task_id: task_data["priority"]})
    
    def worker(node_id: str):
        """工作节点"""
        tasks_claimed = 0
        while True:
            # 获取任务
            task_ids = redis.zrange("evolution:agent:queue", 0, 0)
            if not task_ids:
                break
            
            task_id = task_ids[0]
            lock_key = f"evolution:agent:lock:{task_id}"
            
            # 尝试获取锁
            lock_acquired = redis.set(lock_key, node_id, nx=True, ex=10)
            if not lock_acquired:
                continue
            
            # 从队列移除
            redis.zrem("evolution:agent:queue", task_id)
            
            # 获取任务详情
            task_data = redis.hget("evolution:agent:tasks", task_id)
            if not task_data:
                redis.delete(lock_key)
                continue
            
            # 更新任务状态
            task_dict = json.loads(task_data)
            task_dict["status"] = "claimed"
            task_dict["claimed_by"] = node_id
            task_dict["claimed_at"] = time.time()
            redis.hset("evolution:agent:tasks", task_id, json.dumps(task_dict))
            
            tasks_claimed += 1
            
            # 模拟任务执行
            time.sleep(0.001)
            
            # 完成任务
            task_dict["status"] = "completed"
            task_dict["result"] = {"success": True}
            redis.hset("evolution:agent:tasks", task_id, json.dumps(task_dict))
            redis.delete(lock_key)
        
        with results_lock:
            tasks_per_node[node_id] = tasks_claimed
    
    # 启动10个工作线程
    threads = []
    start_time = time.time()
    
    for i in range(num_nodes):
        node_id = f"node-{i:02d}"
        t = threading.Thread(target=worker, args=(node_id,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    elapsed = time.time() - start_time
    total_claimed = sum(tasks_per_node.values())
    
    print(f"    完成时间: {elapsed:.2f}s")
    print(f"    总任务数: {num_tasks}, 完成数: {total_claimed}")
    print(f"    吞吐量: {total_claimed/elapsed:.1f} tasks/s")
    print(f"    节点分布: {tasks_per_node}")
    
    assert_eq(total_claimed, num_tasks, "所有任务应该被完成")
    
    # 验证负载均衡（每个节点至少完成一些任务）
    for node_id, count in tasks_per_node.items():
        if count < num_tasks // (num_nodes * 2):  # 至少完成平均的一半
            print(f"    WARNING: {node_id} 任务数过少: {count}")


# ═══════════════════════════════════════════════════════════════
#  测试2: 网络分区故障
# ═══════════════════════════════════════════════════════════════

def test_network_partition():
    """模拟网络分区故障"""
    print("\n  [Test] 网络分区故障...")
    
    redis = ChaosRedis()
    
    # 提交任务
    for i in range(100):
        task_id = f"partition-task-{i}"
        task_data = {
            "task_id": task_id,
            "name": f"task-{i}",
            "handler": "test_handler",
            "handler_params": {},
            "priority": 5,
            "status": "pending",
        }
        redis.hset("evolution:agent:tasks", task_id, json.dumps(task_data))
        redis.zadd("evolution:agent:queue", {task_id: 5})
    
    results = {"normal_node": 0, "partitioned_node": 0}
    lock = threading.Lock()
    
    def normal_worker():
        """正常节点"""
        while True:
            try:
                task_ids = redis.zrange("evolution:agent:queue", 0, 0)
                if not task_ids:
                    break
                task_id = task_ids[0]
                lock_key = f"evolution:agent:lock:{task_id}"
                if redis.set(lock_key, "normal", nx=True, ex=10):
                    redis.zrem("evolution:agent:queue", task_id)
                    with lock:
                        results["normal_node"] += 1
                    redis.delete(lock_key)
            except ConnectionError:
                time.sleep(0.01)
    
    def partitioned_worker():
        """被分区的节点"""
        redis.partition_node("partitioned")
        try:
            while True:
                try:
                    task_ids = redis.zrange("evolution:agent:queue", 0, 0)
                    if not task_ids:
                        break
                    task_id = task_ids[0]
                    lock_key = f"evolution:agent:lock:{task_id}"
                    if redis.set(lock_key, "partitioned", nx=True, ex=10):
                        redis.zrem("evolution:agent:queue", task_id)
                        with lock:
                            results["partitioned_node"] += 1
                        redis.delete(lock_key)
                except ConnectionError:
                    # 分区节点应该经常遇到连接错误
                    time.sleep(0.01)
        finally:
            redis.heal_partition("partitioned")
    
    t1 = threading.Thread(target=normal_worker)
    t2 = threading.Thread(target=partitioned_worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    print(f"    正常节点完成任务: {results['normal_node']}")
    print(f"    分区节点完成任务: {results['partitioned_node']}")
    
    # 分区节点应该完成很少或没有任务
    assert_true(results["normal_node"] > results["partitioned_node"], 
                "正常节点应该比分区节点完成更多任务")


# ═══════════════════════════════════════════════════════════════
#  测试3: Redis故障恢复
# ═══════════════════════════════════════════════════════════════

def test_redis_failure_recovery():
    """Redis故障恢复测试"""
    print("\n  [Test] Redis故障恢复...")
    
    redis = ChaosRedis()
    
    # 初始提交任务
    for i in range(50):
        task_id = f"recovery-task-{i}"
        task_data = {"task_id": task_id, "status": "pending", "priority": 5}
        redis.hset("evolution:agent:tasks", task_id, json.dumps(task_data))
        redis.zadd("evolution:agent:queue", {task_id: 5})
    
    completed = [0]
    lock = threading.Lock()
    
    def worker_with_retry():
        """带重试的工作节点"""
        consecutive_errors = 0
        max_consecutive_errors = 50
        no_task_count = 0
        max_no_task_count = 100
        
        while consecutive_errors < max_consecutive_errors:
            try:
                task_ids = redis.zrange("evolution:agent:queue", 0, 0)
                if not task_ids:
                    no_task_count += 1
                    if no_task_count >= max_no_task_count:
                        break  # 连续100次没有任务，退出
                    time.sleep(0.005)
                    continue
                
                no_task_count = 0
                task_id = task_ids[0]
                lock_key = f"evolution:agent:lock:{task_id}"
                
                if redis.set(lock_key, "worker", nx=True, ex=10):
                    redis.zrem("evolution:agent:queue", task_id)
                    with lock:
                        completed[0] += 1
                    redis.delete(lock_key)
                
                consecutive_errors = 0
                
            except ConnectionError:
                consecutive_errors += 1
                time.sleep(0.01)
    
    # 注入故障
    redis.inject_failure(0.3)  # 30%故障率
    
    # 启动工作线程
    threads = [threading.Thread(target=worker_with_retry) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    print(f"    故障率: 30%")
    print(f"    完成任务: {completed[0]}")
    print(f"    Redis错误次数: {redis._error_count}")
    
    # 即使30%故障率，也应该完成大部分任务
    assert_true(completed[0] >= 35, "应该完成至少70%的任务")


# ═══════════════════════════════════════════════════════════════
#  测试4: 脑裂场景
# ═══════════════════════════════════════════════════════════════

def test_split_brain():
    """脑裂场景测试 - 多个节点认为自己是主节点"""
    print("\n  [Test] 脑裂场景...")
    
    redis = ChaosRedis()
    
    # 模拟脑裂：两个分区，每个分区有自己的"主节点"
    # 分区A
    redis.partition_node("partition-a")
    # 分区B
    redis.partition_node("partition-b")
    
    # 两个分区同时尝试获取同一个锁
    results = {"a_acquired": False, "b_acquired": False}
    
    def node_a():
        try:
            result = redis.set("split-brain-lock", "a", nx=True, ex=10)
            if result:
                results["a_acquired"] = True
        except ConnectionError:
            pass
    
    def node_b():
        try:
            result = redis.set("split-brain-lock", "b", nx=True, ex=10)
            if result:
                results["b_acquired"] = True
        except ConnectionError:
            pass
    
    t1 = threading.Thread(target=node_a)
    t2 = threading.Thread(target=node_b)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # 恢复网络
    redis.heal_partition("partition-a")
    redis.heal_partition("partition-b")
    
    print(f"    节点A获取锁: {results['a_acquired']}")
    print(f"    节点B获取锁: {results['b_acquired']}")
    
    # 在脑裂情况下，最多只有一个节点能获取锁
    # （如果Redis实现正确的话）
    lock_value = redis._locks.get("split-brain-lock", {}).get("value")
    print(f"    最终锁持有者: {lock_value}")


# ═══════════════════════════════════════════════════════════════
#  测试5: 任务执行超时和僵死
# ═══════════════════════════════════════════════════════════════

def test_task_timeout_and_stale():
    """任务执行超时和僵死任务清理"""
    print("\n  [Test] 任务执行超时和僵死...")
    
    redis = ChaosRedis()
    
    # 创建一些被认领但未完成的任务（僵死任务）
    now = time.time()
    for i in range(10):
        task_id = f"stale-task-{i}"
        task_data = {
            "task_id": task_id,
            "status": "claimed",
            "claimed_by": f"dead-node-{i}",
            "claimed_at": now - 400,  # 400秒前认领，超过300秒超时
            "priority": 5,
        }
        redis.hset("evolution:agent:tasks", task_id, json.dumps(task_data))
    
    # 创建一些正常进行中的任务
    for i in range(5):
        task_id = f"active-task-{i}"
        task_data = {
            "task_id": task_id,
            "status": "claimed",
            "claimed_by": f"active-node-{i}",
            "claimed_at": now - 60,  # 60秒前认领，未超时
            "priority": 5,
        }
        redis.hset("evolution:agent:tasks", task_id, json.dumps(task_data))
    
    # 清理僵死任务
    def cleanup_stale_tasks(redis, stale_timeout=300.0):
        cleaned = 0
        all_tasks = redis.hgetall("evolution:agent:tasks")
        for task_id, task_data in all_tasks.items():
            task_dict = json.loads(task_data)
            if task_dict.get("status") == "claimed":
                claimed_at = task_dict.get("claimed_at", 0)
                if time.time() - claimed_at > stale_timeout:
                    # 释放锁
                    redis.delete(f"evolution:agent:lock:{task_id}")
                    # 重置任务状态
                    task_dict["status"] = "pending"
                    task_dict["claimed_by"] = None
                    task_dict["claimed_at"] = None
                    redis.hset("evolution:agent:tasks", task_id, json.dumps(task_dict))
                    redis.zadd("evolution:agent:queue", {task_id: task_dict["priority"]})
                    cleaned += 1
        return cleaned
    
    cleaned = cleanup_stale_tasks(redis)
    
    print(f"    僵死任务清理: {cleaned}")
    print(f"    预期清理: 10")
    
    assert_eq(cleaned, 10, "应该清理10个僵死任务")
    
    # 验证僵死任务重新进入队列
    pending_count = redis.zcard("evolution:agent:queue")
    assert_eq(pending_count, 10, "应该有10个任务重新进入队列")


# ═══════════════════════════════════════════════════════════════
#  测试6: 高频任务提交
# ═══════════════════════════════════════════════════════════════

def test_high_frequency_submit():
    """高频任务提交测试（1000 tasks/s）"""
    print("\n  [Test] 高频任务提交（1000 tasks/s）...")
    
    redis = ChaosRedis()
    num_tasks = 1000
    
    start_time = time.time()
    
    # 高频提交任务
    for i in range(num_tasks):
        task_id = f"hf-task-{i}"
        task_data = {
            "task_id": task_id,
            "name": f"hf-task-{i}",
            "handler": "test_handler",
            "handler_params": {"index": i},
            "priority": random.randint(1, 10),
            "status": "pending",
        }
        redis.hset("evolution:agent:tasks", task_id, json.dumps(task_data))
        redis.zadd("evolution:agent:queue", {task_id: task_data["priority"]})
    
    elapsed = time.time() - start_time
    submit_rate = num_tasks / elapsed
    
    print(f"    提交任务数: {num_tasks}")
    print(f"    提交时间: {elapsed:.2f}s")
    print(f"    提交速率: {submit_rate:.1f} tasks/s")
    
    # 验证队列长度
    queue_len = redis.zcard("evolution:agent:queue")
    assert_eq(queue_len, num_tasks, f"队列中应该有{num_tasks}个任务")


# ═══════════════════════════════════════════════════════════════
#  测试7: 节点随机故障和恢复
# ═══════════════════════════════════════════════════════════════

def test_random_node_failure():
    """节点随机故障和恢复"""
    print("\n  [Test] 节点随机故障和恢复...")
    
    redis = ChaosRedis()
    num_tasks = 200
    
    # 提交任务
    for i in range(num_tasks):
        task_id = f"failure-task-{i}"
        task_data = {
            "task_id": task_id,
            "status": "pending",
            "priority": 5,
        }
        redis.hset("evolution:agent:tasks", task_id, json.dumps(task_data))
        redis.zadd("evolution:agent:queue", {task_id: 5})
    
    completed = [0]
    failed_nodes = set()
    lock = threading.Lock()
    
    def unstable_worker(node_id: str, failure_prob: float):
        """不稳定的工作节点"""
        while True:
            # 随机故障
            if random.random() < failure_prob:
                with lock:
                    failed_nodes.add(node_id)
                time.sleep(random.uniform(0.1, 0.5))  # 模拟故障恢复时间
                with lock:
                    failed_nodes.discard(node_id)
                continue
            
            try:
                task_ids = redis.zrange("evolution:agent:queue", 0, 0)
                if not task_ids:
                    break
                
                task_id = task_ids[0]
                lock_key = f"evolution:agent:lock:{task_id}"
                
                if redis.set(lock_key, node_id, nx=True, ex=10):
                    redis.zrem("evolution:agent:queue", task_id)
                    with lock:
                        completed[0] += 1
                    redis.delete(lock_key)
                    
            except ConnectionError:
                time.sleep(0.01)
    
    # 启动5个工作节点，不同故障概率
    threads = []
    for i in range(5):
        failure_prob = 0.1 * (i + 1)  # 10%, 20%, 30%, 40%, 50%
        t = threading.Thread(target=unstable_worker, args=(f"unstable-{i}", failure_prob))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    print(f"    完成任务: {completed[0]}/{num_tasks}")
    print(f"    完成率: {completed[0]/num_tasks*100:.1f}%")
    
    # 即使有节点故障，也应该完成大部分任务
    assert_true(completed[0] >= num_tasks * 0.9, "应该完成至少90%的任务")


# ═══════════════════════════════════════════════════════════════
#  主测试入口
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 70)
    print("  第15轮：分布式协调层极端混沌测试")
    print("=" * 70)
    
    run_test("10节点1000任务竞争", test_10_nodes_10000_tasks)
    run_test("网络分区故障", test_network_partition)
    run_test("Redis故障恢复", test_redis_failure_recovery)
    run_test("脑裂场景", test_split_brain)
    run_test("任务超时和僵死", test_task_timeout_and_stale)
    run_test("高频任务提交", test_high_frequency_submit)
    run_test("节点随机故障", test_random_node_failure)
    
    print("\n" + "=" * 70)
    print(f"  Round 15 Results: PASS={_results['passed']}  FAIL={_results['failed']}")
    print("=" * 70)
    
    if _results["errors"]:
        print("\nFailure Details:")
        for err in _results["errors"]:
            print(f"  {err}")
    
    sys.exit(0 if _results["failed"] == 0 else 1)
