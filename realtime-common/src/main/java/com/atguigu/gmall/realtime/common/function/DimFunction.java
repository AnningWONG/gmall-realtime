package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;

/**
 * ClassName: DimFunction
 * Package: com.atguigu.gmall.realtime.common.function
 * Description:
 *      维度关联需要实现的接口
 *      将DimMapFunction模板中不确定的逻辑定义为抽象方法
 *      将DimMapFunction模板中的抽象方法放到这个接口中
 *      再让DimMapFunction实现这个接口
 *      创建DimMapFunction对象时实现接口中的抽象方法
 * @Author Wang Anning
 * @Create 2024/4/28 10:09
 * @Version 1.0
 */
public interface DimFunction<T> {
    // 向主流数据补充维度属性的方法
    void addDim(T obj, JSONObject dimJsonObj);
    // 获取表名的方法
    String getTableName();
    // 获取行键的方法
    String getRowKey(T obj);
}
