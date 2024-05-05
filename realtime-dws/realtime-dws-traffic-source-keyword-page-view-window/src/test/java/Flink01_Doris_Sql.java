import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Flink01_Doris_Sql
 * Package: PACKAGE_NAME
 * Description:
 *      Flink SQL读写Doris数据
 * @Author Wang Anning
 * @Create 2024/4/24 10:01
 * @Version 1.0
 */
public class Flink01_Doris_Sql {
    public static void main(String[] args) {
        // TODO 1. 基本环境
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(1);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2. 从Doris读取数据
        tableEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode SMALLINT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                "    )   " +
                "    WITH (  " +
                "      'connector' = 'doris',  " +
                "      'fenodes' = 'hadoop102:7030',  " +
                "      'table.identifier' = 'test.table1',  " +
                "      'username' = 'root',  " +
                "      'password' = 'aaaaaa'  " +
                ")  ");
        tableEnv.sqlQuery("select * from flink_doris").execute().print();

        // TODO 3. 向Doris写入数据

        tableEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode INT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'hadoop102:7030', " +
                "  'table.identifier' = 'test.table1', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")  ");

        tableEnv.executeSql("insert into flink_doris values(33, 3, '深圳', 3333)");


    }
}
