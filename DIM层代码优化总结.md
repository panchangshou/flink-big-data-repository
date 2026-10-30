# DIM 层代码优化总结

## 一、优化背景

优化前，整个 DIM 层逻辑都写在一个 `DimApp.java` 里（约 358 行），包含环境准备、checkpoint、KafkaSource、ETL、FlinkCDC、HBase 建表、广播处理、HBase Sink 等所有代码，且大量使用匿名内部类。问题是：职责混乱、重复代码多、无法复用、排查困难。

优化后按"基类 + 工具类 + 独立算子 + 常量/实体"拆分，模块职责清晰，也为后续 DWD、DWS 层复用打基础。

## 二、优化后的代码结构

```text
gmall2024-realtime
├── realtime-common
│   └── src/main/java/com/atguigu/gmall/realtime/common
│       ├── base/BaseApp.java              # 作业基类（模板方法）
│       ├── bean/TableProcessDim.java      # 配置表实体
│       ├── constant/Constant.java         # 全局常量
│       └── util
│           ├── FlinkSourceUtil.java       # KafkaSource / MySqlSource 构建
│           ├── HBaseUtil.java             # HBase 连接与读写
│           └── JdbcUtil.java              # MySQL 连接与通用查询
└── realtime-dim
    └── src/main/java/com/atguigu/gmall/realtime/dim
        ├── app/DimApp.java                # DIM 主流程
        └── function
            ├── TableProcessFunction.java  # 广播关联处理
            └── HBaseSinkFunction.java     # 写 HBase
```

## 三、具体优化点

### 1. 抽取作业基类 BaseApp（模板方法）

`realtime-common/.../base/BaseApp.java`

- 把"环境准备、checkpoint、重启策略、KafkaSource、提交作业"这些所有实时作业都需要的骨架代码集中到 `start(port, parallelism, ckAndGroupId, topic)`。
- 子类只需实现 `handle(env, kafkaStrDS)`，专注业务逻辑。
- `DimApp` 的 `main` 精简为一行：

```java
new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
```

- 好处：后续 DWD/DWS 作业直接继承，checkpoint 参数、Kafka 消费方式、重启策略全部统一，不会各写一套。

### 2. 抽取工具类

#### FlinkSourceUtil

`realtime-common/.../util/FlinkSourceUtil.java`

- `getKafkaSource(topic, groupId)`：统一 KafkaSource 构建，包括 broker、反序列化器（处理空消息）、`latest` 起始位置。
- `getMySqlSource(database, table)`：统一 FlinkCDC MySqlSource 构建，包括 SSL 参数、`StartupOptions.initial()`。
- 好处：source 配置只维护一份，DIM 及其他层共用。

#### HBaseUtil

`realtime-common/.../util/HBaseUtil.java`

- `getHBaseConnection()` / `closeHBaseConnection()`：统一连接创建与释放。
- `createHbaseTable()`：判断表是否存在，存在则跳过，不存在则按列族建表。
- `dropHBaseTable()`：先 disable 再 delete。
- `putRow()`：按列族写入，自动跳过空值。
- `delRow()`：按 rowKey 删除。
- 好处：HBase API 细节与业务代码解耦，DIM 层只关心"建表/删表/写数据"。

#### JdbcUtil

`realtime-common/.../util/JdbcUtil.java`

- `getMySQLConnection()` / `closeMySQLConnection()`：统一连接管理。
- `queryList(conn, sql, clz, isUnderlineToCamel)`：通用查询，利用 `ResultSetMetaData` + `BeanUtils` 反射自动映射到实体类，可选下划线转驼峰。
- 好处：优化前在 `open()` 里手写 `PreparedStatement`、`ResultSet`、逐列 `put` 再 `toJavaObject`，现在一行调用完成。

#### Constant

`realtime-common/.../constant/Constant.java`

- 集中 Kafka broker、topic、MySQL 地址/账号、HBase 命名空间、DWD 各 topic 常量。
- 好处：避免魔法值散落，环境变更只改一处。

### 3. 实体类 TableProcessDim

`realtime-common/.../bean/TableProcessDim.java`

- 用 Lombok `@Data`、`@NoArgsConstructor`、`@AllArgsConstructor` 替代手写 getter/setter/构造器。
- 字段与配置表一一对应，作为广播状态和 HBase 写入的载体。

### 4. 算子逻辑抽成独立 Function 类

#### TableProcessFunction

`realtime-dim/.../function/TableProcessFunction.java`

- 从优化前 `DimApp` 内的匿名 `BroadcastProcessFunction` 抽出。
- 职责：主流与广播流关联、维度判断、字段裁剪、补 `type`。
- 构造器注入 `MapStateDescriptor`，可测试、可复用。
- 保留 `configMap` 预加载：`open()` 中用 `JdbcUtil` 查配置表，解决"配置流晚于业务流"的问题；广播状态优先、`configMap` 兜底。
- `deleteNotNeedColumns` 作为私有静态方法留在类内，职责内聚。

#### HBaseSinkFunction

`realtime-dim/.../function/HBaseSinkFunction.java`

- 从优化前 `DimApp` 内的匿名 `RichSinkFunction` 抽出。
- 职责：连接生命周期管理、按操作类型删除或写入 HBase。
- 与 `TableProcessFunction` 一样，逻辑独立后便于单独排查。

### 5. DimApp 内部方法抽取

优化前所有逻辑堆在 `main` 中；优化后拆成四个私有方法：

| 方法 | 职责 |
|------|------|
| `etl` | Kafka JSON 解析与过滤 |
| `readTableProcess` | FlinkCDC 读取配置表并转实体 |
| `creatHBaseTable` | 根据配置建表/删表 |
| `connect` | 广播、connect、关联处理 |

`handle` 方法只剩 6 行调用，主流程一目了然。

### 6. 广播状态 + configMap 双保险

- 广播状态用于保存最新配置，支持配置表增删改的实时生效。
- `configMap` 在算子 `open()` 时通过 JDBC 全量预加载，保证配置流尚未到达时业务数据也能被识别。
- 删除配置时同时清理广播状态和 `configMap`，避免脏配置残留。
- 查找顺序：广播状态优先，`configMap` 兜底。

### 7. 字段裁剪方式优化

优化前：新建 `JSONObject`，用 `getString` 逐字段拷贝，字段不存在时会写入 `null`。
优化后：

```java
List<String> columnList = Arrays.asList(sinkColumns.split(","));
Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
```

直接在原对象上删除多余字段，保留原始数据类型，避免无中生有的 `null` 字段。

### 8. checkpoint 路径参数化

优化前固定为 `hdfs://hadoop102:8020/ck`；优化后：

```java
env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck" + ckAndGroupId);
```

- 每个作业使用独立目录（如 `/ckdim_app`），互不干扰。
- 同时 `ckAndGroupId` 也作为 Kafka 消费组名，便于按作业管理偏移量。
- 注意：路径变化后首次运行需要 HDFS 可创建目录，且 NameNode 不能处于安全模式。

### 9. Maven 多模块拆分

- `realtime-common`：基类、工具类、常量、实体，供各层共用。
- `realtime-dim`：只放 DIM 业务代码。
- 好处：公共代码一处维护，DWD/DWS 直接依赖 `realtime-common`。

## 四、优化前后对比

| 维度 | 优化前 | 优化后 |
|------|--------|--------|
| 文件规模 | 单个 `DimApp.java` 约 358 行 | `DimApp` 188 行 + 独立 Function/Util |
| 环境与 checkpoint | 写在 main 中，无法复用 | `BaseApp` 统一封装 |
| Kafka/CDC Source | 匿名内部类硬编码 | `FlinkSourceUtil` 统一构建 |
| HBase 操作 | 直接调用 HBase API | `HBaseUtil` 封装 |
| MySQL 查询 | 手写 JDBC + 逐列赋值 | `JdbcUtil.queryList` 通用映射 |
| 广播处理 | 匿名内部类 | 独立 `TableProcessFunction` |
| Sink | 匿名内部类 | 独立 `HBaseSinkFunction` |
| 常量 | 散落各处的魔法值 | `Constant` 集中管理 |
| 复用性 | 仅 DIM 可用 | 各层均可复用 |

## 五、仍可继续优化的点

1. **调试输出**：`HBaseSinkFunction`、`HBaseUtil` 中大量 `System.out.println`，生产环境应改为日志框架（slf4j），避免性能损耗和日志刷屏。
2. **异常处理**：`HBaseSinkFunction.invoke` 捕获异常后仅打印、不抛出，会导致数据静默丢失；建议至少记录日志并按策略抛出。
3. **HBase 连接数**：当前 1 个建表算子 + 4 个 Sink 并行度会创建 5 个连接；可考虑连接复用或连接池（本地模式收益更明显）。
4. **Admin 频繁创建**：`createHbaseTable` / `dropHBaseTable` 每条配置都 `getAdmin()`；配置量小可接受，量大时可复用 `Admin` 或批量处理。
5. **无用代码**：`BaseApp` 中 `String groupId = "dim_app_group"` 未使用；`conf.setInteger("rest.port", port)` 创建后未传给环境，端口配置实际未生效；`DimApp` 存在重复 import 与未使用 import，建议清理。
6. **配置表名硬编码**：`readTableProcess` 和 `TableProcessFunction.open` 中数据库名/表名重复出现，可抽到 `Constant`。
7. **configMap 更新**：`processBroadcastElement` 新增/更新配置时只写了广播状态，没有同步 `configMap`；虽然查询时广播状态优先，但状态恢复后 `configMap` 可能仍是旧值，建议同步更新。
8. **本地启动慢**：Windows 首次创建 HBase 连接约 38 秒，属于类加载/杀毒软件扫描开销，不是代码问题；可将 `.m2` 和 IDEA 目录加入 Defender 排除项，或提交到集群运行。

## 六、本次优化验证过的运行问题

| 现象 | 原因 | 处理 |
|------|------|------|
| 作业启动报 `SafeModeException` | NameNode 处于安全模式，`/ckdim_app` 无法创建 | `hdfs dfsadmin -safemode leave`，或等待自动退出 |
| 控制台长时间无维度数据 | Kafka 从 `latest` 开始且 `topic_db` 无新消息 | 触发业务数据写入，或临时改 `earliest` 重跑 |
| 16 条"表已存在"延迟很久才输出 | Windows 冷启动首次建 HBase 连接约 38 秒 | 本地环境问题，集群运行正常 |
