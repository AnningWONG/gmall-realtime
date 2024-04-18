package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.atguigu.gmall.realtime.common.constant.Constant.*;
import static com.atguigu.gmall.realtime.common.constant.Constant.MYSQL_PASSWORD;

/**
 * ClassName: JdbcUtil
 * Package: com.atguigu.gmall.realtime.common.util
 * Description:
 *      从遵循JDBC规范的数据库中查数据
 * @Author Wang Anning
 * @Create 2024/4/17 15:20
 * @Version 1.0
 */
public class JdbcUtil {

    // 查询，将查询到的每条数据都封装为1个对象，放到集合中作为结果集返回
    // 类后面声明的泛型模板在静态方法中不能使用，所以泛型模板在方法中声明
    public static <T> List<T> queryList(Connection conn,
                                        String querySql,
                                        Class<T> tClass,
                                        boolean... isUnderlineToCamel) throws Exception {
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰
        // 根据参数确定是否执行下划线转驼峰
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        List<T> result = new ArrayList<>();
        // 1. 预编译
        PreparedStatement preparedStatement = conn.prepareStatement(querySql);
        // 2. 执行查询, 获得结果集
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        // 3. 解析结果集, 把数据封装到一个 List 集合中
        while (resultSet.next()) {
            // 遍历到一行数据, 把这个行数据封装到一个 T 类型的对象中
            T t = tClass.newInstance(); // 使用反射创建一个 T 类型的对象
            // 遍历这一行的每一列数据，注意JDBC的偏移量从1开始
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                // 获取列名
                // 获取列值
                String name = metaData.getColumnLabel(i);
                Object value = resultSet.getObject(name);
                // 表中的表名字段是下划线格式，Bean中的属性名是驼峰形式
                // 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
                if (defaultIsUToC) {
                    name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                }

                // 为对象属性逐一赋值t.name=value
                BeanUtils.setProperty(t, name, value);
            }
            result.add(t);
        }
        return result;
    }

    public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException {
        // 加载驱动 获取 jdbc 连接
        Class.forName(MYSQL_DRIVER);
        return DriverManager.getConnection(MYSQL_URL, MYSQL_USER_NAME, MYSQL_PASSWORD);
    }

    public static void closeConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}
