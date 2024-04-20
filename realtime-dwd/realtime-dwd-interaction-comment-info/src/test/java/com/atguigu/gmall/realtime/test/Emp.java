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

public class Emp {
    public Integer empno;
    public String ename;
    public Integer deptno;
    public Long ts;
}
