package com.atguigu.app.dws;

/*
Description: 互动域 课程-用户粒度评价各窗口汇总表
需求说明如下：（注意 review_stars 5 为好评）
统计周期	统计粒度	指标	说明
当日	课程	用户平均评分	平均每个用户的评分
当日	课程	评价用户数	参与评价的用户数
当日	课程	好评率	好评用户数/参与评价的用户数

实体类 CourseUserReviewBean

--ClickHouse建表语句：
drop table if exists dws_course_user_review_window;
create table if not exists dws_course_user_review_window
(
    stt            DateTime COMMENT '窗口起始时间',
    edt            DateTime COMMENT '窗口结束时间',
    course_id   String COMMENT '课程id',
    course_name   String COMMENT '课程名称',
    review_starts_count UInt64 COMMENT '用户累积评分',
    user_ct UInt64 COMMENT '参与评价的用户数',
    starts_user_ct UInt64 COMMENT '好评用户数',
    ts             UInt64 COMMENT '时间戳'
) engine = ReplacingMergeTree(ts)
    partition by toYYYYMMDD(stt)
    order by (stt, edt, course_id);

*/

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.CourseUserReviewBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

//不足：reviewStars的值为null??
//数据流：web/app -> Mysql -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程 序：Mock -> Mysql -> Maxwell -> Kafka(ZK) -> DimApp -> Kafka(ZK) -> Dws34_CourseUserReviewWindow(Redis Phoenix(HBase ZK HDFS)) -> ClickHouse(ZK)
public class Dws34_CourseUserReviewWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.读取Kafka DWD层review_info主题数据
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("dwd_review_add", "CourseUserReview"), WatermarkStrategy.noWatermarks(), "kafka-source");

//        {"course_id":194,"create_time":"2023-05-18 23:07:38","user_id":32,"id":21018,"review_stars":2}
//        {"course_id":46,"create_time":"2023-05-18 23:07:39","user_id":659,"id":21019,"review_stars":5}
        //TODO 3.转化为 CourseUserReviewBean 对象
        SingleOutputStreamOperator<CourseUserReviewBean> beanDS = kafkaDS.map(new MapFunction<String, CourseUserReviewBean>() {
            @Override
            public CourseUserReviewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                //获取评分
                Long stars = jsonObject.getLong("review_stars");

                return CourseUserReviewBean.builder()
                        .courseId(jsonObject.getString("course_id"))
//                        .userId(jsonObject.getString("user_id"))
                        .reviewStartsCount(stars)
                        .userCt(1L)
                        .startsUserCt(5L==stars?1L:0L)
                        .ts(jsonObject.getLong("create_time"))
                        .build();
            }
        });

        //TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<CourseUserReviewBean> beanWithWMDS = beanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<CourseUserReviewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<CourseUserReviewBean>() {
            @Override
            public long extractTimestamp(CourseUserReviewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 5.分组 开窗 聚合
        SingleOutputStreamOperator<CourseUserReviewBean> reduceDS = beanWithWMDS.keyBy(CourseUserReviewBean::getCourseId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<CourseUserReviewBean>() {
                    @Override
                    public CourseUserReviewBean reduce(CourseUserReviewBean value1, CourseUserReviewBean value2) throws Exception {

                        value1.setReviewStartsCount(value1.getReviewStartsCount() + value2.getReviewStartsCount());
                        value1.setUserCt(value1.getUserCt() + value2.getUserCt());
                        value1.setStartsUserCt(value1.getStartsUserCt() + value2.getStartsUserCt());

                        return value1;
                    }
                }, new WindowFunction<CourseUserReviewBean, CourseUserReviewBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<CourseUserReviewBean> input, Collector<CourseUserReviewBean> out) throws Exception {

                        CourseUserReviewBean next = input.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        out.collect(next);

                    }
                });

        reduceDS.print("reduceDS>>>");
        //TODO 6.关联维表 DIM_COURSE_INFO 得到 COURSE_NAME
        SingleOutputStreamOperator<CourseUserReviewBean> resultDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<CourseUserReviewBean>("DIM_COURSE_INFO") {
                    @Override
                    public String getKey(CourseUserReviewBean input) throws Exception {
                        return input.getCourseId();
                    }

                    @Override
                    public void join(CourseUserReviewBean input, JSONObject dimInfo) throws Exception {

                        input.setCourseName(dimInfo.getString("COURSE_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 7.写出到 dws_course_user_review_window
        resultDS.print("resultDS>>>");

        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_course_user_review_window values(?,?,?,?,?,?,?,?)"));

        //TODO 8.启动
        env.execute("Dws34_CourseUserReviewWindow");

    }
}
