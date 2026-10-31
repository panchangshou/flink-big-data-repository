package com.atguigu.gmall.realtime.dwd.log.split;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
//import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
//import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;

import static com.alibaba.fastjson.JSON.parseObject;


/**
 * 日志分流
 * 需要启动的进程
 * zk,kafka,flume
 *
 * * KafkaSource:从kafka主题中读取数据
 *  *             通过手动维护偏移量，保证消费的精准一次
 *  *
 *  * KafkaSink:向Kafka主题中写入数据，也可以保证写入的精准一次，需要做如下操作
 *  *           开启检查点
 *  *           .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 *  *           .setTransactionalIdPrefix("dwd_base_log_")
 *  *           .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")//在source端
 *  *           在消费端，需要设置消费的隔离级别为读已提交 .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
 */
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中数据类型转换，做ETL
        //定义标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = parseObject(jsonStr);
                            //错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流     页面日志-主流
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            //如果转换的时候，发生了异常，说明不是是标准的json，属于脏数据，将其放到侧输出流中
                            ctx.output(dirtyTag, jsonStr);
                        }

                    }
                }

        );
        jsonObjDS.print("标准json：");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("脏数据：");

        //把测数据中的脏数据往kafka写
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        //TODO 对新老访客标记进行修复
        //根据设备id分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //使用状态编程
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取is_new的值
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        //从状态中获取首次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            //如果is_new的值为1
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }

                        } else {
                            //如果 is_new 的值为 0
                            //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
                            // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }

                        return jsonObj;
                    }

                }
        );
        //fixedDS.print();
        //TODO 分流  错误日志-错误侧输出流err 启动日志-启动侧输出流start 曝光日志-曝光侧输出流displays 动作日志-动作侧输出流actions     页面日志-主流page
        //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        //分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        //先处理错误日志
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            //写入整条日志
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            //启动日志
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            //页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
                            //曝光日志和动作日志是数组形式，需要遍历拿出每个对象
                            //曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("displays", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);

                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());

                                }
                                jsonObj.remove("displays");
                            }
                            //动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                //遍历出每一个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    //定义一个新的JSON对象，用于封装动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    //将动作日志写到动作侧输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            //页面日志写入主流
                            out.collect(jsonObj.toJSONString());

                        }

                    }
                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面：");
        errDS.print("错误：");
        startDS.print("启动：");
        displayDS.print("曝光：");
        actionDS.print("动作：");


        //TODO 将不同流的数据放到不同的kafka主题中
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }
}




























































