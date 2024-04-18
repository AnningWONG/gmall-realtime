package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * ClassName: HBaseSinkFunction
 * Package: com.atguigu.gmall.realtime.common.function
 * Description:
 *      将流中数据写到HBase表中
 * @Author Wang Anning
 * @Create 2024/4/17 14:23
 * @Version 1.0
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tuple2, Context context) throws Exception {
        // 获取当前要同步的业务数据库维度数据
        JSONObject jsonObj = tuple2.f0;
        // 获取当前这条维度数据对应的配置信息
        TableProcessDim tableProcessDim = tuple2.f1;
        // 获取业务数据库中这条维度数据的操作类型
        String type = jsonObj.getString("type");
        // type获取后即可从Json对象中去除了，因为数据即将插入的HBase表中是没有这个type字段的
        jsonObj.remove("type");
        // 获取操作的HBase中的表名和主键
        String sinkTable = tableProcessDim.getSinkTable();
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
        // 同步数据
        if ("delete".equals(type)) {
            // 从HBase中删除对应的维度数据
            HBaseUtil.deleteRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
        } else {
            String sinkFamily = tableProcessDim.getSinkFamily();
            // 将维度数据put到HBase表中
            HBaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObj);
        }
    }
}
