package com.atguigu.gmall.realtime.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseUtil {
    //获取HBase连接
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf=new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //conf.set("hbase.zookeeper.property.clientPort","2181");
        Connection hbaseConn = ConnectionFactory.createConnection();
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

            if(admin.)
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    //删表
    public static void dropHBaseTable(Connection hbaseConn,String namespace,String tableName){
        try(Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
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
}
