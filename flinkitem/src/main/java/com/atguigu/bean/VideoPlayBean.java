package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * ClassName: VideoPlayBean
 * Package: com.atguigu.bean
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/18 15:29
 * @Version 1.2
 */
@Data
@AllArgsConstructor
@Builder
public class VideoPlayBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    //章节id
    @TransientSink
    String chapterId;
    //章节名称
    String chapterName;
    //中间字段
    @TransientSink
    String userId;
    @TransientSink
    String videoId;
    //播放视频数
    @Builder.Default
    Long videoPlayCt=0L;
    //播放时长
    @Builder.Default
    Long palySec=0L;
    //观看人数
    @Builder.Default
    Long userCt=0L;
    // 时间戳
    Long ts;

}
