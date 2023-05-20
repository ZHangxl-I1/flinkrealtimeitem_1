package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * ClassName: TradeOrderCount
 * Package: org.st.bean
 * Description:
 *
 * @Author NoahZH
 * @Create 2023/5/19 22:45
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@Builder
public class TradeOrderCountBean {

    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;

    //'下单总额'
    BigDecimal totalAmount;

    //'下单人数'
    Long userCount;

    //'下单次数'
    Long orderCount;

    // 时间戳
    Long ts;

}
