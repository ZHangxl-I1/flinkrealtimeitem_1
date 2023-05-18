package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;

@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 省份 ID
    String provinceId;

    // 省份名称
    @Builder.Default
    String provinceName = "";

    // 订单 ID
    @TransientSink
    HashSet<String> orderIds;

    // 累计下单次数
    @Builder.Default
    Long orderCount=0L;
    //用户id
    @TransientSink
    String userID;

    // 累计下单人数
    @Builder.Default
    Long userCount = 0L;

    // 累计下单金额
    BigDecimal orderAmount;

    // 时间戳
    Long ts;
}
