package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TestPaperCourseBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

/**
 试卷 考试人数/平均分/平均时长
 课程 考试人数/平均分/平均时长
 主类：TestPaperBean

 --ClickHouse建表语句
 drop table if exists dws_test_paper_course_window;
 create table if not exists dws_test_paper_course_window
 (
 stt                     DateTime COMMENT '窗口起始时间',
 edt                     DateTime COMMENT '窗口结束时间',
 user_count UInt64 COMMENT '考试人数',
 score_count Decimal(38, 20) COMMENT '考试总分',
 dur_sec_count Decimal(38, 20) COMMENT '考试总时长',
 paper_id String comment '试卷id',
 paper_title String comment '试卷名称',
 course_id String comment '课程id',
 course_name String comment '课程名称',
 ts                      UInt64 COMMENT '时间戳'
 )    engine = ReplacingMergeTree(ts)
 partition by toYYYYMMDD(stt)
 order by (stt, edt, paper_id, course_id);

 {"id":"14384","paper_id":"661","user_id":"86","score":"45.0","duration_sec":"901","create_time":"2023-05-18 09:27:58","paper_title":"Java基础测试","course_id":39}

 试卷-分数段 人数
 题目 正确答题次数/答题次数/正确率/正确答题独立用户数/答题独立用户数/正确答题用户占比

 */

//数据流：web/app -> Mysql -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程 序：Mock -> Mysql -> Maxwell -> Kafka(ZK) -> Dwd31_TestPaper -> Kafka(ZK) -> Dws31_TestPaperWindow -> ClickHouse(ZK)
public class Dws_TestPaperCourseWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设定 Table 中的时区为本地时区
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));


        //TODO 2.读取Kafka DWD层加购主题数据
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("dwd_test_paper", "test_paper"), WatermarkStrategy.noWatermarks(), "kafka-source");

        //TODO 3.将数据转化为JavaBean对象
        SingleOutputStreamOperator<TestPaperCourseBean> testPaperBeanDS = kafkaDS.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);

//            return TestPaperBean.builder()
//                    .paperId(jsonObject.getString("paper_id"))
//                    .paperTitle(jsonObject.getString("paper_title"))
//                    .ts(jsonObject.getLong("create_time"))
//                    .paperId(jsonObject.getString("paper_id"))
//                    .userCount(1L)
//                    .scoreCount(jsonObject.getBigDecimal("score"))
//                    .durSecCount(jsonObject.getBigDecimal("duration_sec"))
//                    .build();
            return new TestPaperCourseBean(
                    "",
                    "",
                    1L,
                    jsonObject.getBigDecimal("score"),
                    jsonObject.getBigDecimal("duration_sec"),
                    jsonObject.getString("paper_id"),
                    jsonObject.getString("paper_title"),
                    jsonObject.getString("course_id"),
                    "",
                    jsonObject.getLong("create_time")
            );
        });

        //测试
//        testPaperBeanDS.print("testPaperBeanDS>>>>>");

        //TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<TestPaperCourseBean> testPaperBeanWithWMDS = testPaperBeanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TestPaperCourseBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TestPaperCourseBean>() {
            @Override
            public long extractTimestamp(TestPaperCourseBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 5.分组 开窗 聚合
        SingleOutputStreamOperator<TestPaperCourseBean> reduceDS = testPaperBeanWithWMDS.keyBy(TestPaperCourseBean::getPaperId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TestPaperCourseBean>() {
                    @Override
                    public TestPaperCourseBean reduce(TestPaperCourseBean value1, TestPaperCourseBean value2) throws Exception {

                        value1.setUserCount(value1.getUserCount() + value2.getUserCount());
                        value1.setScoreCount(value1.getScoreCount().add(value2.getScoreCount()));
                        value1.setDurSecCount(value1.getDurSecCount().add(value2.getDurSecCount()));

                        return value1;
                    }
                }, new WindowFunction<TestPaperCourseBean, TestPaperCourseBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TestPaperCourseBean> input, Collector<TestPaperCourseBean> out) throws Exception {
                        TestPaperCourseBean next = input.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        out.collect(next);

                    }
                });

//        reduceDS.print("reduceDS>>>>");

        //TODO 5-1关联维表DIM_COURSE_INFO 获取 COURSE_NAME

        SingleOutputStreamOperator<TestPaperCourseBean> resultDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TestPaperCourseBean>("DIM_COURSE_INFO") {
                    @Override
                    public String getKey(TestPaperCourseBean input) throws Exception {
                        return input.getCourseId();
                    }

                    @Override
                    public void join(TestPaperCourseBean input, JSONObject dimInfo) throws Exception {

                        input.setCourseName(dimInfo.getString("COURSE_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);


        resultDS.print("resultDS>>>>");
        //TODO 6.将数据写出
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_test_paper_course_window values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 7.启动任务
        env.execute("Dws31_TestPaperWindow");

    }
}
