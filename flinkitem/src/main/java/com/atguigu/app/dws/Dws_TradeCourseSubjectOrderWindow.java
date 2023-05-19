package com.atguigu.app.dws;

/*
1.3.1 下单情况统计
需求说明如下:
统计周期	统计粒度	指标	说明
当日	学科	下单次数	略
当日	学科	下单人数	略
当日	学科	下单金额	略
//当日	类别	下单次数	略
//当日	类别	下单人数	略
//当日	类别	下单金额	略
当日	课程	下单人数	略
当日	课程	下单金额	略

思路：订单明细id已经是主键了不会重复，去重并转换为

实体类：TradeCourseSubjectOrderBean

--ClickHouse建表语句
drop table if exists dws_trade_course_subject_order_window;
create table if not exists dws_trade_course_subject_order_window
(
    stt                          DateTime COMMENT '窗口起始时间',
    edt                          DateTime COMMENT '窗口结束时间',
    course_id String comment '课程id',
    course_name String comment '课程名称',
    course_order_user_ct UInt64 comment '课程下单人数',
    course_order_amount Decimal(38, 20) comment '课程下单金额',
    subject_id String comment '学科id',
    subject_name String comment '学科名称',
    sub_order_ct    UInt64 COMMENT '学科下单次数',
    sub_order_user_ct    UInt64 COMMENT '学科下单人数',
    sub_order_amount    Decimal(38, 20) COMMENT '学科下单金额',
    ts                           UInt64 COMMENT '时间戳'
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt, course_id, subject_id);

dwd_trade_order_detail 数据展示：
{"id":"114379","course_id":"379","course_name":"尚硅谷AJAX技术","order_id":"102315","user_id":"349","coupon_reduce":"0.0","final_amount":"200.0","create_time":"2023-05-19 10:20:20","origin_amount":"200.0","order_status":"1001","trade_body":"尚硅谷AJAX技术等1件商品","session_id":"ca047742-51be-474e-a539-cf37e866414a","province_id":"29","expire_time":"2023-05-19 10:35:20"}

 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeCourseSubjectOrderBean;
import com.atguigu.bean.TrafficTimeBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyApplyUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

//数据流：web/app -> Mysql -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程 序：Mock -> Mysql -> Maxwell -> Kafka(ZK) -> TradeOrderDetail -> Kafka(ZK) -> Dws_TradeCourseSubjectOrderWindow(Redis Phoenix(HBase ZK HDFS)) -> ClickHouse(ZK)
public class Dws_TradeCourseSubjectOrderWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.读取Kafka DWD层下单明细主题数据
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("dwd_trade_order_detail", "trade_course_subject_order"), WatermarkStrategy.noWatermarks(), "kafka-source");

        //TODO 3.转换为JSON对象
        //{"id":"114379","course_id":"379","course_name":"尚硅谷AJAX技术","order_id":"102315","user_id":"349","coupon_reduce":"0.0","final_amount":"200.0","create_time":"2023-05-19 10:20:20","origin_amount":"200.0","order_status":"1001","trade_body":"尚硅谷AJAX技术等1件商品","session_id":"ca047742-51be-474e-a539-cf37e866414a","province_id":"29","expire_time":"2023-05-19 10:35:20"}
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {

                return JSON.parseObject(value);
            }
        });

        //TODO 4.提取时间戳生成Watermark 2S乱序
        SingleOutputStreamOperator<JSONObject> jsonObjWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("create_time");
            }
        }));

//        //TODO 5.按照 course_id 分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjWMDS.keyBy(json -> json.getString("course_id"));

        //TODO 5.按照订单明细ID分组
//        KeyedStream<JSONObject, String> keyedDS = jsonObjWMDS.keyBy(json -> json.getString("id"));

        //TODO 6.去重并转换为 TradeCourseSubjectOrderBean 对象
        SingleOutputStreamOperator<TradeCourseSubjectOrderBean> beanDS = keyedDS.flatMap(new RichFlatMapFunction<JSONObject, TradeCourseSubjectOrderBean>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeCourseSubjectOrderBean> out) throws Exception {

                String state = valueState.value();
                long userCt = 0L;
                if (state == null) {
                    userCt = 1L;
                    valueState.update("1");

                    //获取课程下单人数
//                    Long courseOrderUserCt=1L;
                    //获取课程下单金额
                    BigDecimal courseOrderAmount = value.getBigDecimal("final_amount");

                    out.collect(TradeCourseSubjectOrderBean.builder()
                            .courseId(value.getString("course_id"))
                            .courseName(value.getString("course_name"))
                            .userId(value.getString("user_id"))
//                            .courseOrderUserCt(userCt)
                            .courseOrderUserCt(1L)
                            .courseOrderAmount(courseOrderAmount)
                            .subOrderCt(0L)
                            .subOrderUserCt(0L)
                            .subOrderAmount(BigDecimal.ZERO)
                            .ts(value.getLong("create_time"))
                            .build());
                }
            }
        });

        //course_id


//
//        //TODO 7.1关联维表 DIM_COURSE_INFO 得到 SUBJECT_ID
        SingleOutputStreamOperator<TradeCourseSubjectOrderBean> subjectIDDS = AsyncDataStream.unorderedWait(beanDS,
                new DimAsyncFunction<TradeCourseSubjectOrderBean>("DIM_COURSE_INFO") {
                    @Override
                    public String getKey(TradeCourseSubjectOrderBean input) throws Exception {
                        return input.getCourseId();
                    }

                    @Override
                    public void join(TradeCourseSubjectOrderBean input, JSONObject dimInfo) throws Exception {
                        input.setSubjectId(dimInfo.getString("SUBJECT_ID"));

                    }
                }, 60, TimeUnit.SECONDS);
//
        //TODO 7.2关联维表 DIM_BASE_SUBJECT_INFO 得到 SUBJECT_NAME
        SingleOutputStreamOperator<TradeCourseSubjectOrderBean> subjectNMDS = AsyncDataStream.unorderedWait(subjectIDDS,
                new DimAsyncFunction<TradeCourseSubjectOrderBean>("DIM_BASE_SUBJECT_INFO") {
                    @Override
                    public String getKey(TradeCourseSubjectOrderBean input) throws Exception {
                        return input.getSubjectId();
                    }

                    @Override
                    public void join(TradeCourseSubjectOrderBean input, JSONObject dimInfo) throws Exception {
                        input.setSubjectName(dimInfo.getString("SUBJECT_NAME"));

                    }
                }, 60, TimeUnit.SECONDS);



        //TODO 8.按照 course_id 分组 开窗 聚合
        SingleOutputStreamOperator<TradeCourseSubjectOrderBean> resultDS = subjectNMDS.keyBy(TradeCourseSubjectOrderBean::getCourseId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeCourseSubjectOrderBean>() {
                    @Override
                    public TradeCourseSubjectOrderBean reduce(TradeCourseSubjectOrderBean value1, TradeCourseSubjectOrderBean value2) throws Exception {

                        value1.setCourseOrderUserCt(value1.getCourseOrderUserCt() + value2.getCourseOrderUserCt());

                        value1.setCourseOrderAmount(value1.getCourseOrderAmount().add(value2.getCourseOrderAmount()));
                        return value1;
                    }
                }, new WindowFunction<TradeCourseSubjectOrderBean, TradeCourseSubjectOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeCourseSubjectOrderBean> input, Collector<TradeCourseSubjectOrderBean> out) throws Exception {

                        TradeCourseSubjectOrderBean next = input.iterator().next();

                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));


                        //输出数据
                        out.collect(next);




                    }
                });


        //subject_id 分组
        SingleOutputStreamOperator<TradeCourseSubjectOrderBean> reduce = resultDS.keyBy(TradeCourseSubjectOrderBean::getSubjectId)
                .map(new RichMapFunction<TradeCourseSubjectOrderBean, TradeCourseSubjectOrderBean>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .build();

                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("subject-state", String.class);
                        stateDescriptor.enableTimeToLive(ttlConfig);

                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public TradeCourseSubjectOrderBean map(TradeCourseSubjectOrderBean value) throws Exception {

                        String lastDt = valueState.value();

                        String curDt = DateFormatUtil.toDate(value.getTs());

                        if (lastDt == null || !lastDt.equals(curDt)) {
                            value.setSubOrderUserCt(1L);
                            valueState.update(curDt);
                        }
                        value.setSubOrderCt(1L);
                        value.setSubOrderAmount(value.getCourseOrderAmount());

                        return value;


                    }
                })
                .keyBy(TradeCourseSubjectOrderBean::getSubjectId)
                .reduce(new ReduceFunction<TradeCourseSubjectOrderBean>() {
                    @Override
                    public TradeCourseSubjectOrderBean reduce(TradeCourseSubjectOrderBean value1, TradeCourseSubjectOrderBean value2) throws Exception {
                        value1.setSubOrderCt(value1.getSubOrderCt() + value2.getSubOrderCt());

                        value1.setSubOrderUserCt(value1.getSubOrderUserCt() + value2.getSubOrderUserCt());

                        value1.setSubOrderAmount(value1.getSubOrderAmount().add(value2.getSubOrderAmount()));

                        return value1;
                    }
                });


        reduce.print("resultDS>>>");

        reduce.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_course_subject_order_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        //执行
        env.execute("Dws_TradeCourseSubjectOrderWindow");

    }
}
