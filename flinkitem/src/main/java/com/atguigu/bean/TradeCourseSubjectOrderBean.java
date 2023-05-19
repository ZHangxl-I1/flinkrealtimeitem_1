package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * ClassName: TradeCourseSubjectOrderBean
 * Package: com.atguigu.bean
 * Description: 下单情况统计
 *
 * @Author NoahZH
 * @Create 2023/5/19 10:07
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@Builder
public class TradeCourseSubjectOrderBean {

    String stt;
    // 窗口关闭时间
    String edt;
    // 下单独立用户数

    //'课程id'
    String courseId;
    //'课程名称'
    String courseName;
    //用户id
    @TransientSink
    String userId;
    //订单id
    @TransientSink
    String orderId;
    //'课程下单人数'
    Long courseOrderUserCt;
    //'课程下单金额'
    BigDecimal courseOrderAmount;
    //'学科id'
    String subjectId;
    //'学科名称'
    String subjectName;
    //'学科下单次数'
    Long subOrderCt;

    //'学科下单人数'
    Long subOrderUserCt;

    //'学科下单金额
    BigDecimal subOrderAmount;

    // 时间戳
    Long ts;

}
