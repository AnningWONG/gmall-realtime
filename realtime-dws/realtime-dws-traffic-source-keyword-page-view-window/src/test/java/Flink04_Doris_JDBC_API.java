import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClassName: Flink04_Doris_JDBC_Sql
 * Package: PACKAGE_NAME
 * Description:
 *      Flink JDBC Connector API 读写Doris数据
 * @Author Wang Anning
 * @Create 2024/4/25 22:20
 * @Version 1.0
 */
public class Flink04_Doris_JDBC_API {
    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(1);
        // TODO 2. 向Doris写数据
        DataStreamSource<User> userDS = env.fromElements(
                new User(35, 4, "深圳", 10L));
        SinkFunction<User> jdbcSink = JdbcSink.<User>sink(
                "insert into table1 values(?,?,?,?)",
                new JdbcStatementBuilder<User>() {
                    @Override
                    public void accept(PreparedStatement ps, User u) throws SQLException {
                        ps.setInt(1,u.getSiteid());
                        ps.setInt(2,u.getCitycode());
                        ps.setString(3,u.getUsername());
                        ps.setLong(4,u.getPv());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(5000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:19030/test")
                        .withUsername("root")
                        .withPassword("aaaaaa")
                        // 重试的超时时间
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );

        userDS.addSink(jdbcSink);
        // TODO 3. 提交任务
        env.execute();
    }
}
