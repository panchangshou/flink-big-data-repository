package com.atguigu.gmall.realtime.dim.function;

import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * 实现将数据写入HBase
 */

public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            hbaseConn = HBaseUtil.getHBaseConnection();
            System.out.println("Sink open成功, hbaseConn=" + hbaseConn);
        } catch (Exception e) {
            System.out.println("Sink open失败:");
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
        System.out.println("Sink invoke被调用, data=" + tup.f0);
        try {
            JSONObject jsonObj = tup.f0;
            TableProcessDim tableProcessDim = tup.f1;
            String type = jsonObj.getString("type");
            jsonObj.remove("type");
            String sinkTable = tableProcessDim.getSinkTable();
            String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
            if ("delete".equals(type)) {
                HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
            } else {
                String sinkFamily = tableProcessDim.getSinkFamily();
                long t1 = System.currentTimeMillis();
                HBaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObj);
                System.out.println("putRow调用结束, 耗时" + (System.currentTimeMillis() - t1) + "ms");
            }
        } catch (Exception e) {
            System.out.println("Sink写入HBase失败:");
            e.printStackTrace();
        }
    }
}
