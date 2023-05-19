package com.atguigu.bean;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * ClassName: CourseUserReviewBean
 * Package: org.st.bean
 * Description:
 *
 * @Author NoahZH
 * @Create 2023/5/18 22:41
 * @Version 1.0
 */



@Data
@AllArgsConstructor
@Builder
public class CourseUserReviewBean {

    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;

    //课程id
    String courseId;
    //课程名称
    String courseName;

    //用户id
//    @TransientSink
//    String userId;

    //用户累计评分
    Long reviewStartsCount;

    //参与评价的用户数
    Long userCt;

    //好评用户数
    Long startsUserCt;

    // review_stars 评分
    @TransientSink
    Long reviewStars;

    // 时间戳
    Long ts;

    public static void main(String[] args) {
        CourseUserReviewBean build = builder().build();
        System.out.println(build);
    }

}
