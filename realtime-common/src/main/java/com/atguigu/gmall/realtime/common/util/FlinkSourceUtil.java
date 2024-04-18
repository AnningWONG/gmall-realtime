package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Properties;

/**
 * ClassName: FlinkSourceUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *      获取Flink读取数据源的工具类
 * @Author Wang Anning
 * @Create 2024/4/17 14:06
 * @Version 1.0
 */
public class FlinkSourceUtil {
    // 获取KafkaSource
    public static KafkaSource<String> getKafkaSource(String topic,String groupId) {

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                // 消费起始位点：先从消费者提交的位点消费，如果没提交过位点，就从最早的偏移量开始消费
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // 注意：SimpleStringSchema的deserialize方法不允许传入的字节数组为空，从Kafka中读取到空消息时会出现问题
                // .setValueOnlyDeserializer(new SimpleStringSchema())
                // 所以自定义一个反序列化器
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message);
                        }
                        // Flink会把空字符数组自动丢弃，而不会抛异常
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return kafkaSource;
    }
    // 获取MySqlSource
    public static MySqlSource<String> getMySqlSource(String dbname, String tableName) {

        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList(dbname)
                .tableList(tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 起点的配置默认值就是initial
                // .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        return mySqlSource;
    }
}
