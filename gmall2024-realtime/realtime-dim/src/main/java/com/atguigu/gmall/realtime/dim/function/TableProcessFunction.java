package com.atguigu.gmall.realtime.dim.function;

import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * 处理主流业务数据和广播配置数据关联后的逻辑
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {
    //处理主流业务数据，根据维度表名到广播状态中读取配置信息，判断是否为维度
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    //将配置信息加载到configMap中，确保配置表信息比业务信息先来
    //和广播算子一样作用与各个分区，但是不能备份数据，仅仅用于加载配置信息，不能取代广播算子
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表的配置信息预加载到configMap中
        //使用jdbc
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mySQLConnection, "select * from gmall2024_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);


    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<com.alibaba.fastjson.JSONObject, TableProcessDim, Tuple2<com.alibaba.fastjson.JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<com.alibaba.fastjson.JSONObject, TableProcessDim>> out) throws Exception {
        String table = jsonObj.getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        TableProcessDim tableProcessDim = null;

        //从广播状态或configMap中获取配置信息
        if ((tableProcessDim=broadcastState.get(table))!=null
                ||(tableProcessDim=configMap.get(table))!=null) {
            //向下游传递维度数据,只需要传递data
            com.alibaba.fastjson.JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //在向下游传递之前，过滤掉不需要的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);

            //补充对维度的操作类型属性
            String type = jsonObj.getString("type");
            dataJsonObj.put("type", type);

            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
        }
    }

    //处理广播流配置信息，把配置数据放到广播状态中或者删除对应的配置，k:维度表名 v:；一个配置对象
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<com.alibaba.fastjson.JSONObject, TableProcessDim, Tuple2<com.alibaba.fastjson.JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<com.alibaba.fastjson.JSONObject, TableProcessDim>> out) throws Exception {
        String op = tp.getOp();

        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String sourceTable = tp.getSourceTable();
        if ("d".equals(op)) {
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);

        } else {
            //对配置表进行了读取、添加或更新，需要将最新的配置信息放到广播状态中
            broadcastState.put(sourceTable, tp);
        }
    }

    //过滤掉不需要的字段
    private static void deleteNotNeedColumns(com.alibaba.fastjson.JSONObject dataJsonObj, String sinkColumns){
        //将需要的字段放入新创建的对象
        //局限性：如果没有该字段就会为null
        //        String[] columnArr = sinkColumns.split(",");
//        JSONObject newJsonObj = new JSONObject();
//        for(String column:columnArr){
//            newJsonObj.put(column,dataJsonObj.getString(column));
//        }

        //
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();//获取 JSON 的键值对集合 (entrySet)，这里拿到的是 Map.Entry，也就是一对一对的“键值对”（比如 "price": 99.9）
        entrySet.removeIf(entry->!columnList.contains(entry.getKey()));


    }
}
