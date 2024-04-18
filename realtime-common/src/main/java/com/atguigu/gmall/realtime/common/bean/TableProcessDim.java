package com.atguigu.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TableProcessDim
 * Package: com.atguigu.gmall.realtime.common.bean
 * Description:
 *      配置表对象
 * @Author Wang Anning
 * @Create 2024/4/16 15:19
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
    // 来源表名，MySQL业务库中这张表的名字
    String sourceTable;

    // 目标表名，实时数仓（DIM层在HBase中）中这张表的名字
    String sinkTable;

    // 输出字段，实时数仓（DIM层在HBase中）中这张表都有哪些字段
    String sinkColumns;

    // 实时数仓（DIM层在HBase中）中这张表的列族，生产环境下1张表只需要1个列族
    String sinkFamily;

    // 实时数仓（DIM层在HBase中）中这张表的主键字段
    String sinkRowKey;

    // 对于配置表操作类型，是对数仓中DIM层的表的增删改的依据
    String op;

}

