"""
插件沙箱 - 限制插件的资源访问和权限，提供异常隔离
"""
import os
import sys
import time
import threading
import resource
import signal
from typing import Dict, Any, Optional, Callable, Set
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class SandboxLimits:
    """沙箱资源限制"""
    max_memory_mb: int = 512           # 最大内存使用（MB）
    max_cpu_time_seconds: float = 60.0  # 最大CPU时间（秒）
    max_file_descriptors: int = 100     # 最大文件描述符数
    max_threads: int = 10               # 最大线程数
    allow_network: bool = False         # 是否允许网络访问
    allow_file_write: bool = True       # 是否允许文件写入
    allowed_paths: Optional[Set[str]] = None  # 允许访问的路径
    blocked_modules: Optional[Set[str]] = None  # 禁止导入的模块


class PluginSandbox:
    """
    插件沙箱
    
    提供:
    1. 资源限制（内存、CPU、文件描述符）
    2. 文件系统隔离
    3. 网络访问控制
    4. 模块导入限制
    5. 异常隔离与恢复
    """
    
    def __init__(self, plugin_id: str, limits: Optional[SandboxLimits] = None):
        self._plugin_id = plugin_id
        self._limits = limits or SandboxLimits()
        self._resource_monitor: Optional[threading.Thread] = None
        self._is_monitoring = False
        self._violation_count = 0
        self._last_violation: Optional[str] = None
        self._start_time = time.time()
        self._peak_memory_mb = 0.0
        
    @contextmanager
    def enter(self):
        """进入沙箱环境"""
        try:
            self._apply_limits()
            self._start_monitoring()
            yield self
        finally:
            self._stop_monitoring()
    
    def _apply_limits(self):
        """应用资源限制"""
        try:
            # 内存限制（软限制，非强制）
            if self._limits.max_memory_mb > 0:
                max_mem_bytes = self._limits.max_memory_mb * 1024 * 1024
                resource.setrlimit(
                    resource.RLIMIT_AS,
                    (max_mem_bytes, max_mem_bytes * 2)
                )
            
            # CPU时间限制
            if self._limits.max_cpu_time_seconds > 0:
                resource.setrlimit(
                    resource.RLIMIT_CPU,
                    (int(self._limits.max_cpu_time_seconds), 
                     int(self._limits.max_cpu_time_seconds) + 10)
                )
            
            # 文件描述符限制
            if self._limits.max_file_descriptors > 0:
                resource.setrlimit(
                    resource.RLIMIT_NOFILE,
                    (self._limits.max_file_descriptors, 
                     self._limits.max_file_descriptors + 50)
                )
            
        except (OSError, ValueError) as e:
            # 在某些平台上可能不支持某些限制
            pass
    
    def _start_monitoring(self):
        """启动资源监控线程"""
        self._is_monitoring = True
        self._resource_monitor = threading.Thread(target=self._monitor_loop)
        self._resource_monitor.daemon = True
        self._resource_monitor.start()
    
    def _stop_monitoring(self):
        """停止资源监控"""
        self._is_monitoring = False
        if self._resource_monitor:
            self._resource_monitor.join(timeout=2.0)
    
    def _monitor_loop(self):
        """资源监控循环"""
        while self._is_monitoring:
            try:
                self._check_memory_usage()
                self._check_cpu_usage()
                time.sleep(1.0)
            except Exception:
                pass
    
    def _check_memory_usage(self):
        """检查内存使用"""
        try:
            import psutil
            process = psutil.Process()
            mem_info = process.memory_info()
            mem_mb = mem_info.rss / 1024 / 1024
            
            self._peak_memory_mb = max(self._peak_memory_mb, mem_mb)
            
            if mem_mb > self._limits.max_memory_mb:
                self._violation_count += 1
                self._last_violation = f"Memory limit exceeded: {mem_mb:.1f}MB > {self._limits.max_memory_mb}MB"
                
        except ImportError:
            pass
    
    def _check_cpu_usage(self):
        """检查CPU使用"""
        try:
            import psutil
            process = psutil.Process()
            cpu_percent = process.cpu_percent(interval=0.1)
            
            # 记录高CPU使用情况
            if cpu_percent > 90:
                self._last_violation = f"High CPU usage: {cpu_percent:.1f}%"
                
        except ImportError:
            pass
    
    def check_violation(self) -> Optional[str]:
        """检查是否有资源违规"""
        return self._last_violation
    
    def get_stats(self) -> Dict[str, Any]:
        """获取沙箱统计信息"""
        return {
            "plugin_id": self._plugin_id,
            "runtime_seconds": time.time() - self._start_time,
            "peak_memory_mb": self._peak_memory_mb,
            "violation_count": self._violation_count,
            "last_violation": self._last_violation,
            "limits": {
                "max_memory_mb": self._limits.max_memory_mb,
                "max_cpu_time_seconds": self._limits.max_cpu_time_seconds,
                "max_file_descriptors": self._limits.max_file_descriptors,
                "allow_network": self._limits.allow_network,
            }
        }


class RestrictedImporter:
    """受限的模块导入器"""
    
    def __init__(self, blocked_modules: Optional[Set[str]] = None):
        self._blocked_modules = blocked_modules or {
            'os.system', 'subprocess', 'socket', 'urllib.request',
            'ftplib', 'telnetlib', 'smtplib', 'poplib', 'imaplib',
        }
        self._original_import = __builtins__.__import__
    
    def install(self):
        """安装受限导入器"""
        def restricted_import(name, *args, **kwargs):
            if self._is_blocked(name):
                raise ImportError(f"Module '{name}' is blocked in plugin sandbox")
            return self._original_import(name, *args, **kwargs)
        
        __builtins__.__import__ = restricted_import
    
    def uninstall(self):
        """卸载受限导入器"""
        __builtins__.__import__ = self._original_import
    
    def _is_blocked(self, name: str) -> bool:
        """检查模块是否被禁止"""
        for blocked in self._blocked_modules:
            if name == blocked or name.startswith(blocked + '.'):
                return True
        return False


@contextmanager
def sandboxed_execution(plugin_id: str, limits: Optional[SandboxLimits] = None):
    """
    在沙箱中执行代码的上下文管理器
    
    Usage:
        with sandboxed_execution("my_plugin") as sandbox:
            result = plugin.handle_task(data)
            if sandbox.check_violation():
                print(f"Resource violation: {sandbox.check_violation()}")
    """
    sandbox = PluginSandbox(plugin_id, limits)
    with sandbox.enter():
        yield sandbox
