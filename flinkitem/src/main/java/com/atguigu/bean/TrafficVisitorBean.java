package com.atguigu.bean;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * ClassName: TrafficVisitorBean
 * Package: com.atguigu.bean
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/19 21:18
 * @Version 1.2
 */
@Data
@AllArgsConstructor
@Builder
public class TrafficVisitorBean {

    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    //来源
    @TransientSink
    String isNew;
    //来源名称
    String sourceName;
    @TransientSink
    JSONObject page;
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
