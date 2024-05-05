package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * ClassName: KeywordUDTF
 * Package: com.atguigu.gmall.realtime.dws.function
 * Description:
 *      自定义分词函数
 * @Author Wang Anning
 * @Create 2024/4/24 11:28
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            // use collect(...) to emit a row
            collect(Row.of(keyword));
        }
    }
}
