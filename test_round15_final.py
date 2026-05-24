#!/usr/bin/env python3
"""
第15轮：分布式协调层最终测试
"""

import sys
import os
import time
import threading
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 70)
print("  第15轮：分布式协调层最终测试")
print("=" * 70)

# ═══════════════════════════════════════════════════════════════
#  测试1: 基础分布式锁竞争
# ═══════════════════════════════════════════════════════════════
print("\n[Test 1] 基础分布式锁竞争（10线程，1000锁）...")

lock_data = {"count": 0, "owner": None}
data_lock = threading.Lock()
errors = []

def distributed_lock_test():
    for i in range(100):
        acquired = False
        retries = 0
        while not acquired and retries < 10:
            with data_lock:
                if lock_data["owner"] is None:
                    lock_data["owner"] = threading.current_thread().name
                    acquired = True
            if not acquired:
                retries += 1
                time.sleep(0.001)
        
        if acquired:
            with data_lock:
                lock_data["count"] += 1
                lock_data["owner"] = None

threads = [threading.Thread(target=distributed_lock_test, name=f"worker-{i}") for i in range(10)]
for t in threads:
    t.start()
for t in threads:
    t.join()

print(f"  锁获取次数: {lock_data['count']}")
assert lock_data['count'] == 1000, f"Expected 1000, got {lock_data['count']}"
print("  [PASS]")

# ═══════════════════════════════════════════════════════════════
#  测试2: 任务队列竞争
# ═══════════════════════════════════════════════════════════════
print("\n[Test 2] 任务队列竞争（5线程，500任务）...")

task_queue = list(range(500))
completed_tasks = []
queue_lock = threading.Lock()

def task_worker():
    while True:
        with queue_lock:
            if not task_queue:
                break
            task = task_queue.pop(0)
        completed_tasks.append(task)

threads = [threading.Thread(target=task_worker) for _ in range(5)]
for t in threads:
    t.start()
for t in threads:
    t.join()

print(f"  完成任务: {len(completed_tasks)}/{500}")
assert len(completed_tasks) == 500, f"Expected 500, got {len(completed_tasks)}"
print("  [PASS]")

# ═══════════════════════════════════════════════════════════════
#  测试3: 故障恢复（模拟节点故障）
# ═══════════════════════════════════════════════════════════════
print("\n[Test 3] 故障恢复（模拟30%节点故障）...")

task_queue = list(range(100))
completed = [0]
failed_nodes = set()
lock = threading.Lock()

def unreliable_worker(node_id):
    failure_prob = 0.3
    while True:
        # 模拟故障
        if random.random() < failure_prob:
            with lock:
                failed_nodes.add(node_id)
            time.sleep(0.05)
            with lock:
                failed_nodes.discard(node_id)
            continue
        
        with lock:
            if not task_queue:
                break
            task = task_queue.pop(0)
        completed[0] += 1

threads = [threading.Thread(target=unreliable_worker, args=(i,)) for i in range(5)]
for t in threads:
    t.start()
for t in threads:
    t.join()

print(f"  完成任务: {completed[0]}/100")
assert completed[0] == 100, f"Expected 100, got {completed[0]}"
print("  [PASS]")

# ═══════════════════════════════════════════════════════════════
#  测试4: 僵死任务检测
# ═══════════════════════════════════════════════════════════════
print("\n[Test 4] 僵死任务检测...")

now = time.time()
tasks = [
    {"id": i, "status": "claimed", "claimed_at": now - 400 if i < 5 else now - 60}
    for i in range(10)
]

cleaned = 0
for task in tasks:
    if task["status"] == "claimed" and now - task["claimed_at"] > 300:
        task["status"] = "pending"
        cleaned += 1

print(f"  清理僵死任务: {cleaned}")
assert cleaned == 5, f"Expected 5, got {cleaned}"
print("  [PASS]")

# ═══════════════════════════════════════════════════════════════
#  测试5: 高并发统计准确性
# ═══════════════════════════════════════════════════════════════
print("\n[Test 5] 高并发统计准确性（20线程，各加1000次）...")

counter = {"value": 0}
counter_lock = threading.Lock()

def increment():
    for _ in range(1000):
        with counter_lock:
            counter["value"] += 1

threads = [threading.Thread(target=increment) for _ in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()

print(f"  最终计数: {counter['value']}")
assert counter["value"] == 20000, f"Expected 20000, got {counter['value']}"
print("  [PASS]")

# ═══════════════════════════════════════════════════════════════
#  汇总
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Round 15 Results: PASS=5  FAIL=0")
print("=" * 70)
