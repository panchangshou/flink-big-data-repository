# DIM 维度层流程总结

## 一、目标

将 MySQL 业务库 `gmall2024` 中的维度表数据，实时同步到 HBase 的 `gmall2024` 命名空间下，表名统一为 `dim_xxx`。整条链路由配置表驱动：配置表新增/修改/删除一条记录，Flink 作业就自动建表、删表或更新广播配置。

## 二、涉及组件

| 组件 | 作用 |
|------|------|
| MySQL `gmall2024` | 业务库，Maxwell 订阅其 binlog |
| MySQL `gmall2024_config.table_process_dim` | 维度同步配置表，FlinkCDC 订阅 |
| Maxwell | 把业务库 binlog 写入 Kafka `topic_db` |
| Kafka `topic_db` | 业务数据流 |
| Flink `DimApp` | 维度处理主作业 |
| HBase `gmall2024` | 维度数据落地 |
| HDFS `/ckdim_app` | Flink checkpoint 存储 |
| ZooKeeper | Kafka / HBase 依赖 |

## 三、整体数据流

```text
MySQL(gmall2024 业务库)
        │ binlog
        ▼
     Maxwell
        │
        ▼
Kafka topic_db ──► KafkaSource ──► etl ──► JSONObject 业务流 ──────────────┐
                                                                          │
MySQL(gmall2024_config.table_process_dim)                                 │
        │ FlinkCDC(initial)                                               │
        ▼                                                                 │
  JSONObject ──► TableProcessDim ──► 建表/删表 ──► broadcast ──► TableProcessFunction
                                                                          │
                                                    Tuple2<JSONObject, TableProcessDim>
                                                          │               │
                                                          ▼               ▼
                                                       print()      HBaseSinkFunction
                                                                          │
                                                                          ▼
                                                              HBase gmall2024:dim_xxx
```

## 四、配置表结构

`gmall2024_config.table_process_dim` 对应实体类 `TableProcessDim`（`realtime-common/.../bean/TableProcessDim.java`）：

| 字段 | 含义 |
|------|------|
| source_table | 来源业务表名，如 `base_trademark` |
| sink_table | HBase 目标表名，如 `dim_base_trademark` |
| sink_columns | 需要同步的字段，逗号分隔 |
| sink_family | HBase 列族，可多个，逗号分隔 |
| sink_row_key | HBase rowKey 对应的字段名 |
| op | 配置表操作类型：`r` 读取、`c` 插入、`u` 更新、`d` 删除 |

## 五、启动流程（BaseApp）

入口：`DimApp.main` 调用 `new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB)`。

`BaseApp.start`（`realtime-common/.../base/BaseApp.java:24`）统一完成：

1. 创建 `StreamExecutionEnvironment`，设置并行度（4）。
2. 开启 checkpoint：5 秒一次、`EXACTLY_ONCE`、超时 60 秒、取消后保留、两次 checkpoint 最小间隔 2 秒。
3. 设置重启策略：`failureRateRestart(3, 30 天, 3 秒)`。
4. 设置 checkpoint 存储：`hdfs://hadoop102:8020/ck` + `ckAndGroupId`，本作业为 `/ckdim_app`。
5. 设置 `HADOOP_USER_NAME=root`。
6. 通过 `FlinkSourceUtil.getKafkaSource` 创建 KafkaSource：
   - topic：`topic_db`
   - groupId：`ckAndGroupId`（本作业为 `dim_app`）
   - 起始位置：`OffsetsInitializer.latest()`（有新消费组时从最新位置开始）
   - 自定义 `DeserializationSchema`，避免空消息报错
7. 调用子类 `handle(env, kafkaStrDS)` 组装业务逻辑。
8. `env.execute()` 提交作业。

## 六、DimApp 处理步骤

`DimApp.handle`（`realtime-dim/.../app/DimApp.java:57`）按顺序执行：

### 1. 业务流 ETL（etl）

`DimApp.etl`（`DimApp.java:160`）：

- 将 Kafka 中的 JSON 字符串解析为 `JSONObject`。
- 过滤条件：
  - `database == gmall2024`
  - `type ∈ {insert, update, delete, bootstrap-insert}`
  - `data != null && data.length() > 2`
- 满足条件的数据向下游传递。

### 2. 读取配置流（readTableProcess）

`DimApp.readTableProcess`（`DimApp.java:124`）：

- 使用 FlinkCDC `MySqlSource` 读取 `gmall2024_config.table_process_dim`。
- `StartupOptions.initial()`：每次启动先做全量快照，保证 16 条配置都能进入流。
- 并行度固定为 1，保证配置顺序。
- 将 CDC JSON 转成 `TableProcessDim`：
  - `op = d`：从 `before` 中取数据。
  - 其他操作：从 `after` 中取数据。
  - 最后把 `op` 写回对象。

### 3. HBase 建表/删表（creatHBaseTable）

`DimApp.creatHBaseTable`（`DimApp.java:88`）：

- 使用 `RichMapFunction`，`open()` 中创建 HBase 连接，`close()` 中关闭。
- 并行度为 1，逐条处理配置：
  - `op = d`：删表。
  - `op = r` 或 `c`：建表（已存在则跳过）。
  - 其他（`u`）：先删表再建表。
- 表名、列族来自配置对象，命名空间为 `gmall2024`。

### 4. 广播配置流

`DimApp.connect`（`DimApp.java:73`）：

- 定义 `MapStateDescriptor<String, TableProcessDim>`，key 为来源表名。
- 调用 `tpDS.broadcast(mapStateDescriptor)` 得到广播流。
- 业务流与广播流 `connect`，交给 `TableProcessFunction` 处理。

### 5. 关联与过滤（TableProcessFunction）

`TableProcessFunction`（`realtime-dim/.../function/TableProcessFunction.java`）：

- `open()`：通过 `JdbcUtil` 直连 MySQL，把配置表全量预加载到 `configMap`，解决"配置流可能晚于业务流"的问题。
- `processElement()` 处理业务数据：
  1. 取 `table` 字段。
  2. 先查广播状态 `broadcastState.get(table)`，查不到再查 `configMap.get(table)`。
  3. 命中说明是维度表：取 `data` 对象。
  4. 按 `sinkColumns` 裁剪字段（`deleteNotNeedColumns`，使用 `entrySet().removeIf`）。
  5. 把操作类型写入 `data.type`。
  6. 输出 `Tuple2<JSONObject, TableProcessDim>`。
- `processBroadcastElement()` 处理配置变更：
  - `op = d`：从广播状态和 `configMap` 中移除。
  - 其他：写入广播状态（最新配置优先）。

### 6. 打印与写 HBase

- `dimDS.print()`：控制台输出维度数据，便于调试。
- `HBaseSinkFunction`（`realtime-dim/.../function/HBaseSinkFunction.java`）：
  - `open()` 创建 HBase 连接。
  - `invoke()` 中：
    - 取出 `type` 并从 JSON 中移除。
    - 用 `sinkRowKey` 字段拼 rowKey。
    - `delete` → `HBaseUtil.delRow`。
    - 其他 → `HBaseUtil.putRow`，写入配置的列族。

### 7. 历史数据

作业启动后，在虚拟机执行 `mysql_to_kafka.sh`，把历史业务数据写入 Kafka，再由本作业同步进 HBase。

## 七、关键工具类

| 工具类 | 职责 |
|--------|------|
| `FlinkSourceUtil` | 统一构建 KafkaSource、MySqlSource |
| `HBaseUtil` | HBase 连接、建表、删表、putRow、delRow |
| `JdbcUtil` | MySQL 连接、通用查询 `queryList`（反射 + 下划线转驼峰） |
| `Constant` | Kafka、MySQL、HBase 命名空间、topic 等常量 |

## 八、运行注意事项

1. **HDFS 安全模式**：NameNode 刚启动时处于 safe mode，checkpoint 目录无法创建，作业会在 JobMaster 初始化阶段失败。确认 `hdfs dfsadmin -safemode get` 为 OFF 再运行。
2. **checkpoint 目录必须可创建**：路径为 `/ck` + `ckAndGroupId`，新路径首次运行需要 mkdir 权限。
3. **Kafka 从 latest 开始**：新消费组不会回放历史消息。控制台没有维度数据时，先确认 `topic_db` 是否有新消息（Maxwell 是否在产出）。
4. **本地 Windows 冷启动慢**：首次创建 HBase 连接需要加载大量类，实测约 38 秒，16 条"表已存在"会延迟出现；集群运行无此问题。
