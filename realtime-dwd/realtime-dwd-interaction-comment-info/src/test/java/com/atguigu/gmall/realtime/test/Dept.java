package com.atguigu.gmall.realtime.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Felix
 * @date 2024/4/19
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
    public Integer deptno;
    public String dname;
    public Long ts;
}
