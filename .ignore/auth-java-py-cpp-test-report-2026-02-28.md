# Auth Java/Python/C++ Consistency Report (2026-02-28)

## 1. 目标
- 修复 Java Consumer 在首包阶段可能错误走 legacy `FETCH` 的问题。
- 按 `auth_tests_verification.csv` 的 9 topics × 4 场景，执行 Java 36 场景真实验证。
- 对齐并核验 Python/C++/Java 与预期结果的一致性。

## 2. Java 修复内容

### 2.1 代码改动
- `RED-Kafka-2.6.2/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java`
  - `sendFetches()`：对所有 Consumer，`nodeApiVersions == null` 时不入队 fetch，先等待 ApiVersions 协商完成。
  - `initializeCompletedFetch()`：显式处理 `GROUP_AUTHORIZATION_FAILED(30)`，抛 `GroupAuthorizationException`。

### 2.2 Java 回归用例
- `RED-Kafka-2.6.2/clients/src/test/java/org/apache/kafka/clients/consumer/internals/FetcherTest.java`
  - `testAutoAssignedFetchWaitsForApiVersionsAndUsesRedFetch`
  - `testManualAssignedFetchWaitsForApiVersions`
  - `testGroupAuthorizationFailureRaisesGroupAuthorizationException`
  - `testFetchDuringEagerRebalance`
  - `testFetchDuringCooperativeRebalance`

## 3. 实际执行与结果

### 3.1 Java 定向回归
- 日志：`/home/admin/mh/kafka/RED-Kafka-2.6.2/.ignore/java_fetcher_auth_fix_test_2026-02-28.log`
- 结果：5/5 PASSED。

### 3.2 Java 业务矩阵 36 场景（真实执行）
- 执行脚本：`tools/run_auth_csv_tests_java.py`
- 执行输入：`auth_tests_verification.csv`
- 执行日志：`.ignore/auth_tests_verification_java_run.log`
- 结果文件：`auth_tests_verification.java_automated.csv`
- 详情文件：`.ignore/auth_tests_verification_java_details.json`
- 对齐统计：`.ignore/auth_java_alignment_2026-02-28.json`
- 结论：Java `36/36` 场景与预期一致（`mismatch_cases=0`）。

### 3.3 Python/C++ 对齐复核
- Python/C++ 对齐统计：`.ignore/auth_py_cpp_alignment_2026-02-28.json`
- 结论：
  - Python `36/36` 一致
  - C++ `36/36` 一致

## 4. 生成产物
- Java 真实执行结果：`auth_tests_verification.java_automated.csv`
- Java 执行详情：`.ignore/auth_tests_verification_java_details.json`
- Java 对齐统计：`.ignore/auth_java_alignment_2026-02-28.json`
- Java 回归矩阵：`.ignore/auth_case_matrix_java_regression_2026-02-28.csv`
- Python/C++ 矩阵：`.ignore/auth_case_matrix_py_cpp_2026-02-28.csv`
- Python/C++ 对齐统计：`.ignore/auth_py_cpp_alignment_2026-02-28.json`

## 5. 结论
- Java 修复已生效，并通过定向回归验证。
- Java 的 36 个业务场景已真实执行，结果与预期完全一致。
- Python/C++ 与预期也保持完全一致。
