package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.atguigu.gmall.realtime.dim.function.HBaseSinkFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * DIM维度层的处理
 * 需要启动的进程：
 * zk,kafka,maxwell,hdfs,HBase,dimapp,
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001,4,"dim_app",Constant.TOPIC_DB);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对业务流中的数据类型进行转换 jsonstr->jsonobj
        SingleOutputStreamOperator<JSONObject> JSONObjDS = etl(kafkaStrDS);
        //TODO 使用FlinkCDC读取配置表中的配置信息

        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);
        //TODO 根据配置信息在hbase中进行建表或删表操作
        tpDS = creatHBaseTable(tpDS);
        //tpDS.print();
        //TODO 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(tpDS, JSONObjDS);
        //TODO 11.将维度数据写入HBase
        dimDS.addSink(new HBaseSinkFunction());
        //最后在虚拟机执行mysql_to_kafka.sh脚本将历史数据全部写入hbase
    }

    private static @NonNull SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<TableProcessDim> tpDS, SingleOutputStreamOperator<JSONObject> JSONObjDS) {
        //TODO 将配置流中的配置信息进行广播：使用广播状态
        //广播算子，k是string,v是tableprocessdim
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        //TODO 根据业务流中的数据和广播变量中的配置信息进行关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = JSONObjDS.connect(broadcastDS);
        //TODO 处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        dimDS.print();
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> creatHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn=HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){

                            HBaseUtil.createHbaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHbaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }

                        return tp;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2024_config", "table_process_dim");

        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL_Source")
                .setParallelism(1);//后续要做广播状态，要求数据集中有序，否则下游处理会乱
        //Maxwell 的插入数据直接放在 data 里；而 Debezium（Flink CDC 底层）的插入数据放在 after 里，更新操作会把旧数据放在 before，新数据放在 after
        //mysqlStrDS.print();

        //TODO 6.对配置流中的数据类型进行转换 jsonstr->实体类TableProcessDim对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim=null;
                        if("d".equals(op)){
                            //对配置表进行了一次删除操作，从before中获取数据
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);

                        }else{
                            //对配置表进行了读取、添加、修改操作，从after中获取数据
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);

                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        //tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> JSONObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");
                        //将满足要求的数据向下游传递
                        if ("gmall2024".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }

                }
        );

        //JSONObjDS.print();//测试
        return JSONObjDS;
    }
}
