package com.atguigu.bean;

/**
 * ClassName: TestQuestionBean
 * Package: org.st.bean
 * Description:
 *
 * @Author NoahZH
 * @Create 2023/5/19 1:30
 * @Version 1.0
 */

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class TestQuestionBean {

    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;

    //'题目id'
    String questionId;

    //'用户id'
    @TransientSink
    String userId;

//    //是否正确
//    @TransientSink
//    String isCorrect;

    //'正确答题次数'
    Long corrCt;

    //'答题次数'
    Long questionCt;

    //'正确答题独立用户数'
    Long corrUuCt;

    //'答题独立用户数'
    Long questionUuCt;

    // 时间戳
    Long ts;

}
