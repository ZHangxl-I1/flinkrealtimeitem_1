package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TrafficSourceBean;
import com.atguigu.bean.TrafficTimeBean;
import com.atguigu.common.EDUConfig;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * ClassName: TrafficTimeWindow
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/19 11:16
 * @Version 1.2
 */
public class TrafficTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //检查点
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


        //读取kakfa dwd层页面主题数据
        /**
         *  "common": {							--公共信息
         *     "ar": "16",							--地区编码
         *     "ba": "iPhone",						--手机品牌
         *     "ch": "Appstore",					--渠道
         *     "is_new": "1",						--是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0。
         *     "md": "iPhone 8",					--手机型号
         *     "mid": "mid_161",					--设备id
         *     "os": "iOS 13.3.1",					--操作系统
         *     "sc": "2",							--来源
         *     "sid": "9acef85b-067d-49f9-9520-a0dda943304e",	--会话id
         *     "uid": "272",						--会员id
         *     "vc": "v2.1.134"					--APP版本号
         *   },
         *   "page": {							--页面信息
         *     "during_time": 11622,			--持续时间毫秒
         *     "item": "57",					--目标id
         *     "item_type": "course_id",		--目标类型
         *     "last_page_id": "course_list",	--上页类型
         *     "page_id": "course_detail"		--页面id
         *   },
         */
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource(EDUConfig.PAGE_TOPIC, "traffic_time"), WatermarkStrategy.noWatermarks(), "kafka-source");


        //数据转换成json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //按照mid 分组
        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        SingleOutputStreamOperator<TrafficTimeBean> trafficSourceMidDS = keyedByMidDS.map(new RichMapFunction<JSONObject, TrafficTimeBean>() {


            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("mid-state", String.class);

                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public TrafficTimeBean map(JSONObject value) throws Exception {

                String lastDt = valueState.value();
                long uvCt = 0L;
                if (lastDt == null) {
                    uvCt = 1L;
                    valueState.update("1");
                }

                return TrafficTimeBean.builder()
                        .sid(value.getJSONObject("common").getString("sid"))
                        .uvCt(uvCt)
                        .ts(value.getLong("ts"))
                        .build();
            }
        });

        //按照sid分组
        KeyedStream<TrafficTimeBean, String> keyedBySidDS = trafficSourceMidDS.keyBy(TrafficTimeBean::getSid);

        //去重sid
        SingleOutputStreamOperator<TrafficTimeBean> trafficSourceSidDS = keyedBySidDS.map(new RichMapFunction<TrafficTimeBean, TrafficTimeBean>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("sid-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);


            }

            @Override
            public TrafficTimeBean map(TrafficTimeBean value) throws Exception {
                String state = valueState.value();
                if (state == null) {
                    value.setSvCt(1L);
                    valueState.update("1");

                }
                value.setPageCt(1L);
                return value;
            }
        });

        SingleOutputStreamOperator<TrafficTimeBean> reduceDS = trafficSourceSidDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficTimeBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficTimeBean>() {
                    @Override
                    public long extractTimestamp(TrafficTimeBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }))
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficTimeBean>() {
                    @Override
                    public TrafficTimeBean reduce(TrafficTimeBean value1, TrafficTimeBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());

                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());

                        value1.setPageCt(value1.getPageCt() + value2.getPageCt());


                        return value1;
                    }
                }, new AllWindowFunction<TrafficTimeBean, TrafficTimeBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficTimeBean> values, Collector<TrafficTimeBean> out) throws Exception {
                        TrafficTimeBean next = values.iterator().next();

                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setTs(System.currentTimeMillis());

                        //输出数据
                        out.collect(next);

                    }
                });


        reduceDS.print("reduceDS>>>");


        reduceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_time_window values(?,?,?,?,?,?)"));


        env.execute();

    }
}
