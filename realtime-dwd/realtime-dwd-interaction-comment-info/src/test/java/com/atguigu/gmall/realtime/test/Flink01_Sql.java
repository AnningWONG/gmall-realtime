package com.atguigu.gmall.realtime.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Felix
 * @date 2024/4/19
 *                      左表                      右表
 * 内连接          OnCreateAndWrite        OnCreateAndWrite
 * 左外连接        OnReadAndWrite          OnCreateAndWrite
 * 右外连接        OnCreateAndWrite        OnReadAndWrite
 * 全外连接        OnReadAndWrite          OnReadAndWrite
 */
public class Flink01_Sql {
    public static void main(String[] args) {
        // TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.4 设置状态的失效时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 2.从指定的网络端口读取员工数据 并将其转换为动态表
        SingleOutputStreamOperator<Emp> empDS = env
                .socketTextStream("hadoop102", 8888)
                .map(
                        new MapFunction<String, Emp>() {
                            @Override
                            public Emp map(String lineStr) throws Exception {
                                String[] fieldArr = lineStr.split(",");
                                return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                            }
                        }
                );
        tableEnv.createTemporaryView("emp",empDS);

        // TODO 3.从指定的网络端口读取部门数据 并将其转换为动态表
        SingleOutputStreamOperator<Dept> deptDS = env
                .socketTextStream("hadoop102", 8889)
                .map(
                        new MapFunction<String, Dept>() {
                            @Override
                            public Dept map(String lineStr) throws Exception {
                                String[] fieldArr = lineStr.split(",");
                                return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1], Long.valueOf(fieldArr[2]));
                            }
                        }
                );
        tableEnv.createTemporaryView("dept",deptDS);

        // TODO 4.内连接
        // 注意：如果使用普通的内外连接，底层会为参与连接的两张变各自维护一个状态。默认情况下，状态永远不会失效
        // 在生产环境下，一定要设置状态的失效时间。
        // tableEnv.executeSql("select * from emp e join dept d on e.deptno = d.deptno").print();
        // TODO 5.左外连接
        // tableEnv.executeSql("select * from emp e left join dept d on e.deptno = d.deptno").print();


        // TODO 6.右外连接
        // tableEnv.executeSql("select * from emp e right join dept d on e.deptno = d.deptno").print();
        // TODO 7.全外连接
        // tableEnv.executeSql("select * from emp e full join dept d on e.deptno = d.deptno").print();
        // TODO 8. 将左外连接的结果写到Kafka主题
        // 8.1 创建动态表，和Kafka主题映射
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  `empno` integer,\n" +
                "  `ename` string,\n" +
                "  `deptno` integer,\n" +
                "  `dname` string,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        // 8.2 写入
        tableEnv.executeSql("insert into emp_dept select e.empno,e.ename,d.deptno,d.dname " +
                "from emp e left join dept d on e.deptno = d.deptno");
    }
}
