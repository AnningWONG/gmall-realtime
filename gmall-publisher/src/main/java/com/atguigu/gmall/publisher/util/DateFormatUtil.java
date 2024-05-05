package com.atguigu.gmall.publisher.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * ClassName: DateFormatUtil
 * Package: com.atguigu.gmall.publisher.util
 * Description:
 *      日期工具类
 * @Author Wang Anning
 * @Create 2024/4/29 14:44
 * @Version 1.0
 */
public class DateFormatUtil {
    public static Integer now() {
        String date = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.parseInt(date);
    }
}
