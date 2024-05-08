package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: BaseApp
 * Package: com.atguigu.gmall.realtime.common.base
 * Description:
 *      所有Flink应用的基类，模板方法设计模式，在父类中定义完成某一个功能的核心算法骨架（步骤）
 *      其中有些步骤在父类中没有办法实现，延迟到子类中实现
 *      好处：约定执行功能的模板，在不改变核心骨架的前提下，每一个子类都可以有自己不同的实现
 * @Author Wang Anning
 * @Create 2024/4/17 14:32
 * @Version 1.0
 */
public abstract class BaseApp {
    public void start (int port, int parallelism, String ckAndGroupId, String topic) {


        // TODO 1. 基本环境准备
        // 1.1 指定流处理环境
        Configuration config = new Configuration();
        // 配置Flink JobManager的REST服务端口
        // 这个端口是用来让外部服务或者客户端通过REST API与Flink JobManager通信的
        config.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // env.disableOperatorChaining();

        // 1.2 设置并行度
        env.setParallelism(parallelism);



        // TODO 2. 检查点相关设置
        // 2.1 开启检查点，指定检查点时间间隔，默认模式就是barrier对齐的精准一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2.2 设置检查点超时时间，应该长一点，因为默认检查点失败1次整个Job就会失败，此处设置为1min
        checkpointConfig.setCheckpointTimeout(60000L);
        // 2.3 设置job取消后，检查点是否保留，保留后就可以从检查点恢复Job
        checkpointConfig.
                setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间最小时间间隔，这是前1个检查点的结束时间和后1个检查点的开始时间最小间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 2.5 设置Job的重启策略（非检查点配置，因为检查点失败会导致Job失败，所以此处增加此配置）
        // 每30天只允许失败3次，重试间隔3s
        // env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3000L)));
        // 3次重试机会，间隔3s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        // 2.6 设置状态后端，指定检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        // 数仓每一层一个消费者组，检查点每一层保存到一个文件夹下
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck" + ckAndGroupId);
        // 2.7 设置操作Hadoop用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");




        // TODO 3. 从Kafka主题中读取数据
        // 3.1 声明消费主题和消费者组
        String groupId = ckAndGroupId;
        // 3.2 创建消费者对象KafkaSource
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, groupId);
        // 3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        // TODO 4. 处理逻辑
        handle(env,kafkaStrDS);
        // TODO 5. 提交作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}
