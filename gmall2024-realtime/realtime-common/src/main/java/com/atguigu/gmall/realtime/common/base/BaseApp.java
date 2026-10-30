package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.doris.flink.sink.EscapeHandler.handle;

/**
 * FlinkAPI的基类
 * 模板方法设计模式：在父类中定义完成某一个功能的核心算法骨架(步骤)，有些步骤在父类中没有办法实现，需要延迟到子类中去完成
 *  *          好处：约定了模板
 *  *               在不改变父类核心算法骨架的前提下，每个子类都可以有自己不同的实现
 */
public abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);//精准一次
        env.getCheckpointConfig().setCheckpointTimeout(60000L);//检查点超时时间
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//job取消后检查点是否保留
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);//最小时间间隔

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));//重启策略

        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck"+ckAndGroupId);
        System.setProperty("HADOOP_USER_NAME", "root");
        //TODO 3.从Kafka的topic_db主题中读取业务数据

        String groupId = "dim_app_group";
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        //TODO 4.处理逻辑
        //模板方法设计模式
        handle(env,kafkaStrDS);
        //TODO 5.提交作业
        env.execute();
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) ;
}
