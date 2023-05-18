package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficSourceBean;
import com.atguigu.common.EDUConfig;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyApplyUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: TrafficSourceWindow02
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/17 19:38
 * @Version 1.2
 */
public class TrafficSourceWindow02 {


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
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource(EDUConfig.PAGE_TOPIC, "traffic_source"), WatermarkStrategy.noWatermarks(), "kafka-source");

        //数据转换成json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //按照mid 分组
        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        SingleOutputStreamOperator<TrafficSourceBean> trafficSourceMidDS = keyedByMidDS.map(new RichMapFunction<JSONObject, TrafficSourceBean>() {


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
            public TrafficSourceBean map(JSONObject value) throws Exception {

                String lastDt = valueState.value();
                long uvCt = 0L;
                if (lastDt == null) {
                    uvCt = 1L;
                    valueState.update("1");
                }

                return TrafficSourceBean.builder()
                        .pageId(value.getJSONObject("page").getString("page_id"))
                        .sid(value.getJSONObject("common").getString("sid"))
                        .sc(value.getJSONObject("common").getString("sc"))
                        .uvCt(uvCt)
                        .ts(value.getLong("ts"))
                        .durSum(value.getJSONObject("page").getLong("during_time"))
                        .build();
            }
        });



        //按照sid分组
        KeyedStream<TrafficSourceBean, String> keyedBySidDS = trafficSourceMidDS.keyBy(TrafficSourceBean::getSid);

        //去重sid
        SingleOutputStreamOperator<TrafficSourceBean> trafficSourceSidDS = keyedBySidDS.map(new RichMapFunction<TrafficSourceBean, TrafficSourceBean>() {
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
            public TrafficSourceBean map(TrafficSourceBean value) throws Exception {
                String state = valueState.value();
                if (state == null) {
                    value.setSvCt(1L);
                    valueState.update("1");

                }
                return value;
            }
        });



        //按page_id分组
        KeyedStream<TrafficSourceBean, String> keyedByPageIdDS = trafficSourceSidDS.keyBy(TrafficSourceBean::getPageId);

        SingleOutputStreamOperator<TrafficSourceBean> trafficSourcePageIdDS = keyedByPageIdDS.map(new RichMapFunction<TrafficSourceBean, TrafficSourceBean>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("mid-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public TrafficSourceBean map(TrafficSourceBean value) throws Exception {
                String state = valueState.value();
                String pageId = value.getPageId();
                String sid = value.getSid();

                String key = pageId+"-"+sid;

                if (state == null || !state.equals(key)) {
                    value.setPageCt(1L);
                    valueState.update(key);
                }

                return value;
            }

        });



        SingleOutputStreamOperator<TrafficSourceBean> reduceDS = trafficSourcePageIdDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficSourceBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficSourceBean>() {
                    @Override
                    public long extractTimestamp(TrafficSourceBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }))
                .keyBy(TrafficSourceBean::getSc)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficSourceBean>() {
                    @Override
                    public TrafficSourceBean reduce(TrafficSourceBean value1, TrafficSourceBean value2) throws Exception {


                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());

                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());

                        Long pageOne=0L;
                        if (value1.getPageCt()==1L){
                            pageOne++;
                        }

                        if (value2.getPageCt()==1L){
                            pageOne=pageOne+1;
                        }
                        value1.setPageOneCt(pageOne);

                        value1.setPageOneCt(value1.getPageOneCt()+value2.getPageOneCt());

                        value1.setPageCt(value1.getPageCt() + value2.getPageCt());

                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());

                        return value1;
                    }
                }, new WindowFunction<TrafficSourceBean, TrafficSourceBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TrafficSourceBean> input, Collector<TrafficSourceBean> out) throws Exception {
                        //取出数据
                        TrafficSourceBean next = input.iterator().next();

                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setTs(System.currentTimeMillis());

                        //输出数据
                        out.collect(next);



                    }
                });

        reduceDS.print("reduceDS>>>>");


        env.execute();

    }
}
