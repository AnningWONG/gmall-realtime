package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * ClassName: BeanToJsonStrMapFunc
 * Package: com.atguigu.gmall.realtime.common.function
 * Description:
 *      实体类对象转换为json字符串
 * @Author Wang Anning
 * @Create 2024/4/25 11:20
 * @Version 1.0
 */
public class BeanToJsonStrMapFunc <T> implements MapFunction<T,String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        String jsonStr = JSON.toJSONString(bean, config);
        return jsonStr;
    }
}
