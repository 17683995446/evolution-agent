# 持续进化Agent

一个**执行与优化双并行、零空闲、永久循环**的智能Agent系统。

## 核心特点

- **一开即全速运转**：不存在等待、休眠、闲置状态
- **双并行架构**：主线执行 + 副线优化同时进行
- **自动进化**：任务越做越熟练，流程越跑越精简
- **异常自愈**：局部故障自动恢复，全局循环永不停止
- **全程闭环**：执行与进化深度绑定

## 四层架构

```
┌─────────────────────────────────────────────────────────┐
│                    常驻守护层                            │
│         全程保活，异常自动修复重启，杜绝停机              │
├─────────────────────────────────────────────────────────┤
│                    任务执行层                            │
│         核心干活模块，持续输出成果，满负荷推进             │
├─────────────────────────────────────────────────────────┤
│                    智能优化层                            │
│         实时复盘执行数据，找短板、改流程、提效率           │
├─────────────────────────────────────────────────────────┤
│                    同步落地层                            │
│         优化规则即时生效，直接融入下一轮任务执行           │
└─────────────────────────────────────────────────────────┘
```

## 项目结构

```
evolution_agent/
├── shared/                           # 共享基础层
│   ├── shared_data_models.py         # 核心数据模型
│   ├── global_configuration_loader.py # 全局配置
│   └── unified_logging_service.py    # 统一日志+事件总线
├── execution/                        # 执行支撑层
│   ├── execution_data_collector.py   # 执行数据采集器
│   ├── optimization_rule_repository.py # 优化规则仓库
│   └── exception_recovery_handler.py # 异常恢复处理器
├── core/                             # 核心业务层
│   ├── core_task_execution_engine.py # 任务执行引擎
│   └── intelligent_optimization_analyzer.py # 智能优化分析器
├── sync/                             # 同步守护层
│   ├── rule_sync_landing_bridge.py   # 规则同步桥接器
│   └── resident_guardian_manager.py  # 常驻守护管理器
├── examples/                         # 示例任务
│   └── example_task_definitions.py   # 7种任务处理器
├── evolution_agent_main_entry.py     # 主入口
├── requirements.txt                  # 依赖
├── stress_test_comprehensive_suite.py # 压力测试
├── deep_architecture_test_suite.py   # 深层架构测试
└── chaos_test_extreme_suite.py       # 混沌测试
```

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 运行（无限循环）
python evolution_agent_main_entry.py

# 运行指定周期数
python evolution_agent_main_entry.py --cycles 100 --tasks 20

# 运行压力测试
python stress_test_comprehensive_suite.py

# 运行混沌测试
python chaos_test_extreme_suite.py
```

## 测试覆盖

| 测试套件 | 测试数 | 覆盖场景 |
|---------|--------|---------|
| 压力测试 | 17 | 并发、异常、边界、长时间 |
| 深层测试 | 12 | 架构级、竞态条件、内存 |
| 混沌测试 | 12 | 极端条件、随机故障、资源耗尽 |

## 永久循环流程

```
启动Agent
    ↓
持续执行任务（主线）
    ↓ ←─────────────────┐
实时采集执行数据          │
    ↓                    │
分析不足、梳理优化方向     │
    ↓                    │
更新任务执行规则          │
    ↓                    │
新规则直接投入下一轮执行   │
    ↓                    │
继续执行+持续复盘优化 ─────┘
```

## 技术栈

- Python 3.9+
- PyYAML（配置解析）
- threading（并发控制）
- concurrent.futures（线程池）

## License

MIT
