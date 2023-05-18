package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * ClassName: TestPaperBean
 * Package: org.st.bean
 * Description: 各试卷考试统计
 *
 * @Author NoahZH
 * @Create 2023/5/18 9:57
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@Builder
public class TestPaperCourseBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 考试人数
//    @Builder.Default
    Long userCount;

    // 考试总分
//    @Builder.Default
//    BigDecimal scoreCount= BigDecimal.ZERO;
    BigDecimal scoreCount;

    // 考试总时长
//    @Builder.Default
//    BigDecimal durSecCount=BigDecimal.ZERO;
    BigDecimal durSecCount;

    // 试卷id
//    @TransientSink
    String paperId;

    // 试卷名称
//    @TransientSink
    String paperTitle;

    //课程id
    String courseId;

    //课程名称
    String courseName;

    // 时间戳
    Long ts;

}
