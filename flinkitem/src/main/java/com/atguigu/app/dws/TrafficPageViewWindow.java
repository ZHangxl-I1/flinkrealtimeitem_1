package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：web/app -> 日志服务器(log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程 序：Mock -> log - > Flume -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> Dws03_TrafficPageViewWindow -> ClickHouse(ZK)

public class TrafficPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取KafkaDWD层页面日志主题数据
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource("dwd_traffic_page_log", "page_view_221109"),
                WatermarkStrategy.noWatermarks(),
                "kafka-source");

        //TODO 3.转换为JSON对象并过滤出首页和商品详情页的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                //取出页面ID
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    out.collect(jsonObject);
                }
            }
        });

        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.去重Mid，并转换为JavaBean对象
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

            private ValueState<String> homeState;
            private ValueState<String> detailState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .build();

                ValueStateDescriptor<String> homeStateDes = new ValueStateDescriptor<>("home-state", String.class);
                homeStateDes.enableTimeToLive(ttlConfig);
                ValueStateDescriptor<String> detailStateDes = new ValueStateDescriptor<>("detail-state", String.class);
                detailStateDes.enableTimeToLive(ttlConfig);

                homeState = getRuntimeContext().getState(homeStateDes);
                detailState = getRuntimeContext().getState(detailStateDes);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                //取出页面ID
                String pageId = value.getJSONObject("page").getString("page_id");

                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                long homeCt = 0L;
                long detailCt = 0L;

                if ("home".equals(pageId)) {
                    String lastDt = homeState.value();
                    if (lastDt == null || !lastDt.equals(curDt)) {
                        homeCt = 1L;
                        homeState.update(curDt);
                    }
                } else {
                    String lastDt = detailState.value();
                    if (lastDt == null || !lastDt.equals(curDt)) {
                        detailCt = 1L;
                        detailState.update(curDt);
                    }
                }

                if (homeCt == 1L || detailCt == 1L) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "", homeCt, detailCt, ts));
                }
            }
        });

        //TODO 6.提取时间戳生成Watermark
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailWithWMDS = trafficHomeDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficHomeDetailPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 7.开窗、聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailWithWMDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                        TrafficHomeDetailPageViewBean next = values.iterator().next();

                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setTs(System.currentTimeMillis());

                        out.collect(next);
                    }
                });

        //TODO 8.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("Dws03_TrafficPageViewWindow");

    }
}
