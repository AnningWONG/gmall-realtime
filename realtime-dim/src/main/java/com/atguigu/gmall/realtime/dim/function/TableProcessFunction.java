package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * ClassName: TableProcessFunction
 * Package: com.atguigu.gmall.realtime.common.function
 * Description:
 *      处理主流和广播流数据——过滤出维度
 * @Author Wang Anning
 * @Create 2024/4/17 14:18
 * @Version 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    // 用于预加载配置表信息的普通变量
    Map<String, TableProcessDim> configMap = new HashMap<>();
    // 状态描述器
    MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    // 带参数的构造器
    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    // 广播流数据尚未加载到广播状态中，主流就有数据到来，主流的数据可能丢失
    // 用Maxwell做开关？但是离线项目也需要Maxwell，不能关
    // 所以创建open方法，将配置表中的配置信息提前加载到程序中
    @Override
    public void open(Configuration parameters) throws Exception {
        Connection conn = JdbcUtil.getMysqlConnection();
        String sql = "select * from gmall_config.table_process_dim";
        List<TableProcessDim> tableProcessDimList
                = JdbcUtil.queryList(conn, sql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            // 普通变量configMap以业务表表名为键
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeConnection(conn);
    }

    // 实现processElement方法：处理主流业务数据
    // 根据处理的表名，到广播状态中获取对应的配置信息，若获取到配置，说明这条业务数据是维度数据，将数据传递到下游
    @Override
    public void processElement(JSONObject jsonObj,
                               BroadcastProcessFunction<JSONObject,
                                       TableProcessDim,
                                       Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx,
                               Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 先获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState
                = ctx.getBroadcastState(mapStateDescriptor);
        // 从当前处理的业务数据中获取表名
        String table = jsonObj.getString("table");
        // 根据表名到广播状态中获取对应配置对象
        TableProcessDim tableProcessDim = null;
        // 如果对象不为空，说明当前处理的数据是维度，将其中data部分传递到下游
        if ((tableProcessDim = broadcastState.get(table)) != null
                || (tableProcessDim =  configMap.get(table)) != null) {
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            // 向下游传递数据前，将不需要传递的属性过滤掉
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);
            // 注意先过滤再新增type属性
            // 向下游传递数据前，补充对维度表数据的操作类型
            String type = jsonObj.getString("type");
            dataJsonObj.put("type", type);
            // 将当前处理的维度数据，以及这条数据对应的配置对象封装为Tuple2，传递到下游
            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
        }
    }

    // 实现processBroadElement方法
    // 根据流中配置信息更改广播状态
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim,
                                        BroadcastProcessFunction<JSONObject,
                                                TableProcessDim,
                                                Tuple2<JSONObject, TableProcessDim>>.Context ctx,
                                        Collector<Tuple2<JSONObject, TableProcessDim>> out)
            throws Exception {
        // 获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState
                = ctx.getBroadcastState(mapStateDescriptor);
        // 获取配置表操作类型
        String op = tableProcessDim.getOp();
        // 广播状态底层的map以业务表表名为键
        String key = tableProcessDim.getSourceTable();
        if ("d".equals(op)) {
            // 从配置表删除了一条数据，将这条数据对应的配置信息从广播状态删除
            broadcastState.remove(key);
        } else {
            // 配置表进行了C|R|U，将这条数据对应的配置信息放到广播状态中
            broadcastState.put(key, tableProcessDim);
            configMap.put(key,tableProcessDim);
        }
    }
    // 过滤业务表数据中不需要的字段
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> colList = Arrays.asList(sinkColumns.split(","));
        // JSONObject底层实现了Map接口
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        // 不要在增强for中用集合的remove方法删除元素，因为会导致集合的modecount和底层迭代器对象中的modecount不一致
        // next方法校验时发现不一致会抛异常
        /*
        for (Map.Entry<String, Object> entry : entrySet) {
            if (!colList.contains(entry.getKey())) {
                // dataJsonObj.remove(entry.getKey());
                entrySet.remove(entry);
            }
        }*/

        // 而是要调用迭代器的remove方法，会同时更新集合和迭代器对象的modecount属性
        /*
        Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();
        while (iterator.hasNext()) {
            if (!colList.contains(iterator.next().getKey())) {
                iterator.remove();
            }
        }
        */

        // 调用removeif方法即可
        entrySet.removeIf(entry -> !colList.contains(entry.getKey()));

    }
}
