import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: TestParalism
 * Package: PACKAGE_NAME
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/4/19 19:10
 * @Version 1.0
 */
public class TestParallelism {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);


        MySqlSource<String> mySqlSource = FlinkSourceUtil
                .getMySqlSource("gmall_config", "gmall_config.table_process_dim");
        // 5.2 读取数据封装为流，并行度为1，将配置流转换为广播流时并行度也会自动变为1，这里直接创建为1
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);


        mysqlStrDS.print();
        env.execute();
    }
}
