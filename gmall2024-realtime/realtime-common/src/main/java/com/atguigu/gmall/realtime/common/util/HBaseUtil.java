package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;

/**
 * 工具类，封装HBase的常用基础操作
 */

public class HBaseUtil {
    //获取HBase连接
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //conf.set("hbase.zookeeper.property.clientPort","2181");//手动指定端口号
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn;

    }
    //关闭HBase连接
    public static void closeHBaseConnection(Connection connection) throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
    //建表
    public static void createHbaseTable(Connection hbaseConn,String namespace,String tablename,String ... families){
        if(families.length<1){
            System.out.println("至少需要一个列族");
            return;
        }
        try(Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tablename);
            if(admin.tableExists(tableNameObj)){
                System.out.printf("表空间" + namespace + "下的表" + tablename + "已存在");
                System.out.printf("\n");
                return;
            }
            //添加列族
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for(String family:families){
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.printf("表空间" + namespace + "下的表" + tablename + "创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    //删表
    public static void dropHBaseTable(Connection hbaseConn,String namespace,String tableName){
        try(Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            //判断要删除的表是否存在
            if(!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的表空间"  + namespace + "下的表" + tableName + "不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.printf("删除的表空间"  + namespace + "下的表" + tableName + "");


        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    //向表中put数据

    /**
     *
     * @param hbaseConn 连接对象
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey rowkey
     * @param family 列族
     * @param jsonObj 要put的数据
     */
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNotEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间" + namespace + "下的表" + tableName + "中put数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //向表中删除数据

    /**
     *
     * @param hbaseConn
     * @param namespace
     * @param tableName
     * @param rowKey
     */
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try(Table table=hbaseConn.getTable(tableNameObj)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间" + namespace + "下的表" + tableName + "中删除数据成功, rowKey=" + rowKey);
        }catch (IOException e){
            throw new RuntimeException(e);
        }

    }
}
