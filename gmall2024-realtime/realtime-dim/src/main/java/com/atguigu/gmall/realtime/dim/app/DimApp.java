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
 * zk,kafka,maxwell,hdfs,HBase,dimapp
 * 开发流程
 *  *      基本环境准备
 *  *      检查点相关的设置
 *  *      从kafka主题中读取数据
 *  *      对流中数据进行类型转换并ETL     jsonStr->jsonObj
 *  *      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *  *      使用FlinkCDC读取配置表中的配置信息
 *  *      对读取到的配置流数据进行类型转换    jsonStr->实体类对象
 *  *      根据当前配置信息到HBase中执行建表或者删除操作
 *  *          op=d        删表
 *  *          op=c、r     建表
 *  *          op=u        先删表，再建表
 *  *      对配置流数据进行广播---broadcast
 *  *      关联主流业务数据以及广播流配置数据---connect
 *  *      对关联后的数据进行处理---process
 *  *          new TableProcessFunction extends BroadcastProcessFunction{
 *  *              open:将配置信息预加载到程序中，避免主流数据先到，广播流数据后到，丢失数据的情况
 *  *              processElement:对主流数据的处理
 *  *                  获取操作的表的表名
 *  *                  根据表名到广播状态中以及configMap中获取对应的配置信息，如果配置信息不为空，说明是维度，将维度数据发送到下游
 *  *                      Tuple2<dataJsonObj,配置对象>
 *  *                  在向下游发送数据前，过滤掉了不需要传递的属性
 *  *                  在向下游发送数据前，补充操作类型
 *  *              processBroadcastElement:对广播流数据进行处理
 *  *                  op =d  将配置信息从广播状态以及configMap中删除掉
 *  *                  op!=d  将配置信息放到广播状态以及configMap中
 *  *          }
 *  *     将流中数据同步到HBase中
 *  *          class HBaseSinkFunction extends RichSinkFunction{
 *  *              invoke:
 *  *                  type="delete" : 从HBase表中删除数据
 *  *                  type!="delete" :向HBase表中put数据
 *  *          }
 *  *     优化：抽取FlinkSourceUtil工具类
 *  *          抽取TableProcessFunction以及HBaseSinkFunction函数处理
 *  *          抽取方法
 *  *          抽取基类---模板方法设计模式
 *  * 执行流程（以修改了品牌维度表中的一条数据为例）
 *  *      当程序启动的时候，会将配置表中的配置信息加载到configMap以及广播状态中
 *  *      修改品牌维度
 *  *      binlog会将修改操作记录下来
 *  *      maxwell会从binlog中获取修改的信息，并封装为json格式字符串发送到kafka的topic_db主题中
 *  *      DimApp应用程序会从topic_db主题中读取数据并对其进行处理
 *  *      根据当前处理的数据的表名判断是否为维度
 *  *      如果是维度的话，将维度数据传递到下游
 *  *      将维度数据同步到HBase中
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
