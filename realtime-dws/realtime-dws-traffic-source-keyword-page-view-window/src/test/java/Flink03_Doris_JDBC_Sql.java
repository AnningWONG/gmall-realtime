import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Flink03_Doris_JDBC
 * Package: PACKAGE_NAME
 * Description:
 *      Flink JDBC Connector SQL 读写Doris数据
 * @Author Wang Anning
 * @Create 2024/4/25 22:20
 * @Version 1.0
 */
public class Flink03_Doris_JDBC_Sql {
    public static void main(String[] args) {
        // TODO 1. 基本环境
        // 1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 并行度
        env.setParallelism(1);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2. 创建表
        tableEnv.executeSql("CREATE TABLE test_doris_jdbc (\n" +
                "  siteid int,\n" +
                "  citycode int,\n" +
                "  username string,\n" +
                "  pv bigint\n" +
                // "  PRIMARY KEY (siteid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:19030/test',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'table1',\n" +
                "   'username' = 'root'\n," +
                "   'password' = 'aaaaaa'\n" +
                ")");
        // TODO 3. 从Doris读数据
        tableEnv.executeSql("select * from test_doris_jdbc").print();
        // TODO 4. 向Doris写数据
        // tableEnv.executeSql("insert into test_doris_jdbc values(34, 3, '深圳', 3333)");
    }
}
