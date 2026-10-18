package com.atguigu.gmall.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示Flink CDC的使用
 */
public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        //读取mysql中的数据
        //先做全表扫描，再从binlog中获取信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2024_config")
                .tableList("gmall2024_config.t_user")//测试表
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial())//第一次启动创建快照
                //.startupOptions(StartupOptions.earliest())//不创建快照
                //.startupOptions(StartupOptions.latest())//从最新开始
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source")
                .print();

        env.execute("Print Mysql snapshot+Binlog");


    }
}
