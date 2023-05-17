package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;

@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderWindow {
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
    Long orderCount;

    // 累计下单人数
    Long userCount;

    // 累计下单金额
    BigDecimal orderAmount;

    // 时间戳
    Long ts;
}
