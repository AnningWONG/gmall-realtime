package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * ClassName: SqlUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *      Flink SQL工具类
 * @Author Wang Anning
 * @Create 2024/4/20 14:59
 * @Version 1.0
 */
public class SqlUtil {
    // 获取Kafka连接器的连接属性
    public static String getKafkaDDL (String topic, String groupId) {
        // 注意字符串前后留空格
        return  " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                        "  'topic' = '"+ topic +"',\n" +
                        "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"',\n" +
                        "  'properties.group.id' = '"+ groupId +"',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")";
    }
    // 获取HBase连接器的连接属性
    public static String getHBaseDDL (String tableName) {
        return  " WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }
    // 获取Upsert Kafka连接器的连接属性
    public static String getUpsertKafkaDDL (String topic) {
        return  " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ topic +"',\n" +
                "  'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
