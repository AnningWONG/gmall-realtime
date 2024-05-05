package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
            // 创建hadoop的配置对象
            Configuration conf = new Configuration();
            // 通过ZooKeeper创建连接
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
        // HBase的DDL通过Admin进行
        try (Admin admin = hbaseConn.getAdmin()) {
            // 获取TableName对象
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            // 如果表已存在，就不创建
            if (admin.tableExists(tableNameObj)) {
                System.out.println("要创建的" + namespace + "下的表" + tableName + "已经存在");
                return;
            }
            // 表描述器的构造器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                // 列族描述器的构造器
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder
                        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
                // 通过列族描述器将列族设置到表描述器的构造器中
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            // 通过表描述器创建表
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("创建" + namespace + "下的表" + tableName);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 删表
    public static void dropHBaseTable(Connection hbaseConn, String namespace, String tableName) {
        // HBase的DDL通过Admin进行
        try (Admin admin = hbaseConn.getAdmin()) {
            // 获取TableName对象
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            // 如果表不存在，就不删除
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的" + namespace + "下的表" + tableName + "不存在");
                return;
            }
            // HBase删表前要先禁用表
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除" + namespace + "下的表" + tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 向HBase表中put数据
     * @param hbaseConn 连接对象
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey 主键
     * @param family 列族
     * @param jsonObj 要写入的json对象
     */
    public static void putRow(Connection hbaseConn, String namespace,
                              String tableName, String rowKey, String family, JSONObject jsonObj){
        // 获取TableName对象
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        // HBase的DML操作通过Table对象进行
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            // 通过行键创建1个Put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            // json对象的属性名就是列名
            Set<String> cols = jsonObj.keySet();
            // 对于每一列
            for (String col : cols) {
                // 如果值不为空，就加入到Put对象中
                String value = jsonObj.getString(col);
                if (value != null){
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value));
                }
            }
            // 将这1行数据写入HBase表中
            table.put(put);
            System.out.println("向" + namespace + "下的表" + tableName + "中插入行键为" + rowKey + "的数据成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 从HBase表中删除数据
    public static void deleteRow(Connection hbaseConn, String namespace,
                                 String tableName, String rowKey) {
        // 获取TableName对象
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        // HBase的DML操作通过Table对象进行
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            // 通过行键创建1个Delete对象
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            // 将这1行数据删除
            table.delete(delete);
            System.out.println("删除" + namespace + "下的表" + tableName + "中行键为" + rowKey + "的数据成功" );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    // 根据RowKey从表中获取数据
    public static <T> T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey,
                               Class<T> tClass, boolean... isUnderlineToCamel){
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰
        // 根据参数确定是否执行下划线转驼峰
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }
        // 根据库（命名空间 namespace）名和表名，获取TableName对象
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        // 获取Table连接对象，写在try括号里的资源能自动释放
        try (Table table = hbaseConn.getTable(tableNameObj)){
            // 根据行键
            Get get = new Get((Bytes.toBytes(rowKey)));
            // 获取1行数据
            Result result = table.get(get);
            // 1行的数据转换为1个个单元格Cell
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                // 用1个对象保存这1行数据
                T obj = tClass.newInstance();
                // Cell的列名对应对象的属性名，用Cell的列值依次为这个对象的属性赋值
                for (Cell cell : cells) {
                    // 不要用Cell对象的getter方法，Cell对象调用get方法返回的都是保存所有信息的数组
                    String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String colValue = Bytes.toString(CellUtil.cloneValue(cell));
                    // 根据参数进行列名的下划线转驼峰
                    if (defaultIsUToC) {
                        colName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, colName);
                    }
                    // 赋值
                    BeanUtils.setProperty(obj,colName,colValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    // 测试getRow方法
    public static void main(String[] args) throws IOException {
        Connection hbaseConn = getHBaseConnection();
        System.out.println(getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_trademark", "1", JSONObject.class));
        closeHBaseConnection(hbaseConn);
    }


    // 获取异步HBase connection
    public static AsyncConnection getAsyncHBaseConnection () {
        try {
            // 创建hadoop的配置对象
            Configuration conf = new Configuration();
            // 通过ZooKeeper创建连接
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            // 不写2181也可，默认就是2181
            // conf.set("hbase.zookeeper.property.clientPort", "2181");
            // 创建异步连接
            AsyncConnection asyncHBaseConn = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncHBaseConn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 关闭异步HBase connection
    public static void closeAsyncHBaseConnection(AsyncConnection asyncHBaseConn) throws IOException {
        if (asyncHBaseConn != null && asyncHBaseConn.isClosed()) {
            asyncHBaseConn.close();
        }
    }

    // 异步方式根据RowKey从表中获取数据，封装为json对象返回
    public static JSONObject getRowAsync(AsyncConnection asyncHBaseConn,
                                         String namespace,
                                         String tableName,
                                         String rowKey){

        // 根据库（命名空间 namespace）名和表名，获取TableName对象
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        // HBase的DQL（DML）通过Table对象进行
        AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncHBaseConn.getTable(tableNameObj);
        // 根据行键获取Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取查询结果
            Result result = asyncTable.get(get).get();
            // 转化为Cell集合
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                JSONObject jsonObj = new JSONObject();
                // 根据Cell集合中的每一个Cell，为json对象添加属性和属性值
                for (Cell cell : cells) {
                    String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String colValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(colName, colValue);
                }
                // 返回json对象
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
