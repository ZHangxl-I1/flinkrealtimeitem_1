package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * ClassName: TrafficTimeBean
 * Package: com.atguigu.bean
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/19 11:19
 * @Version 1.2
 */
@Data
@AllArgsConstructor
@Builder
public class TrafficTimeBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    //会话id中间计算使用
    @TransientSink
    String sid;
    //独立访客数
    Long uvCt;
    //会话总数
    @Builder.Default
    Long svCt = 0L;
    //页面数
    @Builder.Default
    Long pageCt=0L;
    // 时间戳
    Long ts;
}
