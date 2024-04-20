package com.atguigu.gmall.realtime.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Flink02_Demo
 * Package: com.atguigu.gmall.realtime.test
 * Description:
 *      模拟评论事实表实现
 * @Author Wang Anning
 * @Create 2024/4/20 10:05
 * @Version 1.0
 */
public class Flink02_Demo {
    public static void main(String[] args) {
        // TODO 1. 基本环境准备
        // 1.1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2
        env.setParallelism(1);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2. 检查点 略
        // TODO 3. Kafka主题中读员工数据
        tableEnv.executeSql("CREATE TABLE emp (\n" +
                "  `empno` string,\n" +
                "  `ename` string,\n" +
                "  `deptno` string,\n" +
                "  proc_time as proctime()\n" + // 使用Lookup Join要求其中一张表有处理时间字段
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        // tableEnv.executeSql("select * from emp").print();
        // TODO 4. HBase表读部门数据
        // 缓存维度表数据，前提是维度表不应该经常发生变化
        tableEnv.executeSql("CREATE TABLE dept (\n" +
                " deptno string,\n" +
                " info ROW<dname string>,\n" +
                " PRIMARY KEY (deptno) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'default:t_dept',\n" + // default是默认值，不写也行
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '200',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour',\n" +
                " 'lookup.async' = 'true'\n" + // Flink bug，必须开启异步，否则每一个query都使用同一个Table连接对象查询，线程不安全
                ")");
        // tableEnv.executeSql("select deptno,dname from dept").print();
        // TODO 5. 员工和部门关联
        // 不适合使用普通内外连接进行关联，因为状态失效时间没有办法设置
        // 应该使用Lookup Join，底层实现原理和普通的内外连接完全不同
        // 底层不会为参与Join的两张表维护状态，是以左表进行驱动
        // 左表数据到来时，发送请求和右表进行关联
        // 使用Lookup Join要求其中一张表有处理时间字段
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "  e.empno,e.ename,d.deptno,d.dname\n" +
                "FROM emp AS e\n" +
                "  JOIN dept FOR SYSTEM_TIME AS OF e.proc_time AS d\n" +
                "    ON e.deptno = d.deptno");
        // joinedTable.execute().print();

        // TODO 6. 关联结果写到Kafka主题
        // 6.1 创建动态表，和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE joined_table (\n" +
                "  `empno` string,\n" +
                "  `ename` string,\n" +
                "  `deptno` string,\n" +
                "  `dname` string,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'second',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        // 将数据写入Sink表中，即写入Kafka主题
        // 方式1：
        // joinedTable.executeInsert("joined_table");
        // 方式2：
        // tableEnv.executeSql("insert into joined_table select * from " + joinedTable);

        // 方式3：
        /*
        tableEnv.createTemporaryView("joined_table_tmp",joinedTable);
        tableEnv.executeSql("insert into joined_table select * from joined_table_tmp");
        */
    }
}
