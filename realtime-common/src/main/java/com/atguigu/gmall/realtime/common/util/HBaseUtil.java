package com.atguigu.gmall.realtime.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * ClassName: HBaseUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *      操作HBase的工具类
 * @Author Wang Anning
 * @Create 2024/4/16 15:44
 * @Version 1.0
 */
public class HBaseUtil {
    // 获取HBase connection
    public static Connection getHBaseConnection () {
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            // 不写2181也可，默认就是2181
            // conf.set("hbase.zookeeper.property.clientPort", "2181");

            Connection conn = ConnectionFactory.createConnection(conf);
            return conn;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    // 关闭HBase connection
    public static void closeHBaseConnection(Connection HBaseConn) throws IOException {
        if (HBaseConn != null && HBaseConn.isClosed()) {
            HBaseConn.close();
        }
    }

    // 建表
    public static void createHBaseTable(Connection hbaseConn, String namespace, String tableName, String... families) {
        // HBase中建表必须指定列族，如果配置表这条数据中没有列族信息，就不在HBase中创建这张表
        if (families.length < 1) {
            System.out.println("HBase中建表必须指定列族");
            return;
        }
        // JDK 1.7新特性，写在try括号里的资源会自动释放
        // DDL通过Admin进行
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
                System.out.println("要创建的" + namespace + "下的表" + tableName + "已经存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder
                        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("创建" + namespace + "下的表" + tableName);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 删表
    public static void dropHBaseTable(Connection hbaseConn, String namespace, String tableName) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的" + namespace + "下的表" + tableName + "不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除" + namespace + "下的表" + tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
