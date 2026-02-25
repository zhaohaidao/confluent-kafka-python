# MetricsSender Runtime Payload Diff执行方案（2026-02-25）

## 目标

- 获取 **Python SDK** 与 **C++ SDK** 实际上报到 collector 的 HTTP headers + payload JSON。
- 生成可复现的字段级 diff：
  - `only_in_py`
  - `only_in_cpp`
  - `type_mismatches`
  - `value_mismatches`
- 将静态分析结论升级为运行时证据，优先验证 `config` 字段集合差异是否为主要来源。

## 新增工具

- `tools/metrics_sender_capture_diff.py`
  - `serve`：本地 mock collector，抓取 POST 请求并写入 JSONL
  - `report`：从 JSONL 中选择 Python/C++ 样本并生成 Markdown diff 报告

## 执行步骤（单机同源抓包）

### 1. 启动本地 collector

```bash
source /home/admin/mh/fluss-r/.venv/bin/activate
python tools/metrics_sender_capture_diff.py serve \
  --bind 127.0.0.1 \
  --port 18080 \
  --output .ignore/metrics-captures/runtime-captures-2026-02-25.jsonl
```

说明：
- 建议 Python/C++ 都发到同一个 collector，分别使用不同 path（例如 `/py`、`/cpp`）。
- 该 collector 会自动解 gzip，并保存 headers、解压后 body、`body_json`、`sha256`。

### 2. 让 Python SDK 上报到本地 collector

在 Python 测试或业务进程配置中设置：

```python
conf["metrics.collect.url"] = "http://127.0.0.1:18080/py"
```

建议同时固定环境变量，减少噪声：

```bash
export XHS_ENV=test
export JOB_ENV=
export APPID=diff-test
export XHS_REGION=test-region
export XHS_SERVICE=metrics-diff
export XHS_ZONE=test-zone
```

### 3. 让 C++ SDK 上报到同一个 collector

在 C++ 测试/样例进程配置中设置：

```text
metrics.collect.url=http://127.0.0.1:18080/cpp
```

要求：
- 尽量复用同一 Kafka 集群、同一 topic、同类 client（Producer vs Producer / Consumer vs Consumer）
- 尽量在相近时间窗口内采样

### 4. 生成运行时 diff 报告

```bash
source /home/admin/mh/fluss-r/.venv/bin/activate
python tools/metrics_sender_capture_diff.py report \
  --input .ignore/metrics-captures/runtime-captures-2026-02-25.jsonl \
  --py-path /py \
  --cpp-path /cpp \
  --ignore-key metrics_id \
  --ignore-path $.time \
  --output .ignore/metrics-sender-runtime-payload-diff-report-2026-02-25.md
```

可选参数（按噪声情况逐步增加）：

```bash
--ignore-path $.msg_cnt
--ignore-path $.msg_size
--ignore-path $.tx
--ignore-path $.rx
--ignore-header x-request-id
```

说明：
- 默认已忽略常见易变 headers：`Host`、`Content-Length`、`User-Agent` 等。
- 默认已忽略 `metrics_id`。
- `topics` 列表若包含 `topic_name`，工具会按 `topic_name` 排序后再比较。

## 报告解读建议（优先级）

### P0：先看 `Payload Top-Level Diff`

- 如果大量 `only_in_py` / `only_in_cpp` 是配置项键名，基本可确认：
  - Python `conf dict` vs C++ `Conf::dump()` 是主要差异源

### P1：再看 `Header Diff`

- 重点确认：
  - `biz-type` 是否仅为预期差异
  - `content-encoding` 是否行为一致（gzip 阈值）

### P2：最后看 `Payload Recursive Diff`

- 用于检查：
  - `topics` 结构是否一致
  - `cgrp` 扁平化字段是否一致
  - 是否存在类型漂移（例如 number vs string）

## 建议的后续动作（基于报告结果）

### 若主要差异来自 config keys（高概率）

1. 评估在 Python SDK 暴露 `conf_dump()`（底层 `rd_kafka_conf_dump`）
2. 将 Python `MetricsCollector` 的 config 采集源从用户 `conf dict` 切换到底层 dump
3. 为 Python/C++ 增加 schema contract test（允许忽略动态计数字段）

### 若主要差异来自 header/value 噪声

1. 扩充默认 ignore 列表并沉淀统一 diff 口径
2. 固定采样时机（例如启动后第 N 次发送）
3. 对同一个角色分别做 Producer/Consumer 两份样本

## 风险与注意事项

- 运行时 stats 计数器天然波动，`value_mismatches` 不代表 schema 不兼容。
- 若某一侧 metrics sender 未启用（URL 未生效），report 会报找不到对应 path 的 JSON 记录。
- 如需稳定复现，建议临时将发送周期对齐并控制 workload 节奏。
