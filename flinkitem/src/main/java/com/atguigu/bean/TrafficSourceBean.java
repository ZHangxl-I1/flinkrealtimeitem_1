package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * ClassName: TradeSourceBean
 * Package: com.atguigu.bean
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/17 10:05
 * @Version 1.2
 */
@Data
@AllArgsConstructor
@Builder
public class TrafficSourceBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    //来源
    @TransientSink
    String sc;
    //来源名称
    String sourceName;
    //会话id中间计算使用
    @TransientSink
    String sid;
    //页面id中间计算使用
    @TransientSink
    String pageId;
    //独立访客数
    @Builder.Default
    Long uvCt=0L;
    //会话总数
    @Builder.Default
    Long svCt = 0L;
    //页面数
    @Builder.Default
    Long pageCt=0L;
    //一个页面的会话数
    @TransientSink
    @Builder.Default
    Long pageOne=0L;
    @Builder.Default
    Long pageOneCt=0L;
    // 累计访问时长
    @Builder.Default
    Long durSum=0L;
    // 时间戳
    Long ts;
}
