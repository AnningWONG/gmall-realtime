package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: BaseSqlApp
 * Package: com.atguigu.gmall.realtime.common.base
 * Description:
 *      FlinkSQL应用程序的基类
 * @Author Wang Anning
 * @Create 2024/4/20 15:11
 * @Version 1.0
 */
public abstract class BaseSqlApp {
    public void start (int port,
                       int parallelism,
                       String ck
    ) {
        // TODO 1. 基本环境准备
        // 1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(parallelism);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2. 检查点相关设置

        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        // 2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        // 2.3 job取消后检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 2.4 检查点最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.5 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
//        // 2.6 设置状态后端，指定检查点存储路径
//        env.setStateBackend(new HashMapStateBackend());
//        // 数仓每一层一个消费者组，检查点每一层保存到一个文件夹下
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck" + ck);
//        // 2.7 设置操作Hadoop用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 业务处理
        handle(env, tableEnv);
    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv);
    // 从Kafka topic_db主题读取数据，创建动态表
    // 权限protected就够
    public void readOdsDb(StreamTableEnvironment tableEnv,String groupId) {
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `data` MAP<string,string>,\n" +
                "  `old` MAP<string,string>,\n" +
                "  `ts` bigint,\n" +
                "  `pt` as proctime(),\n" +
                "  `et` as to_timestamp_ltz(ts,0),\n" +
                "  watermark for et as et\n" + // 水位线生成策略：严格递增
                ")" + SqlUtil.getKafkaDDL(Constant.TOPIC_DB,groupId));
        // tableEnv.executeSql("select * from topic_db").print();
    }
    // 从HBase中查询字典数据，创建动态表
    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SqlUtil.getHBaseDDL("gmall:dim_base_dic"));
        // tableEnv.executeSql("select * from base_dic").print();
    }
}
