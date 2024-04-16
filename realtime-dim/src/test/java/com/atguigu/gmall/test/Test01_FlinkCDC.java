package com.atguigu.gmall.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Test01_FlinkCDC
 * Package: com.atguigu.gmall.test
 * Description:
 *      演示Flink CDC
 * @Author Wang Anning
 * @Create 2024/4/16 11:13
 * @Version 1.0
 */
public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 使用Flink SQL CDC必须开启检查点
        // enable checkpoint
        env.enableCheckpointing(3000L);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(13306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process_dim") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                // startupOptions：
                // 默认值initial：第一次从头读取binlog，后续从binlog末尾开始向后读
                // earliest：从头读，如果binlog信息不全的话，读到的仍然是不全的数据
                // latest：从binlog末尾开始向后读
                // .startupOptions(StartupOptions.initial())
                .build();

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print(); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
