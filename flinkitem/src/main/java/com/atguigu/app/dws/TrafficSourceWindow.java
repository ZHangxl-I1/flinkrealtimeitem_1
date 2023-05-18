package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficSourceBean;
import com.atguigu.common.EDUConfig;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyApplyUtil;
import com.atguigu.utils.MyKafkaUtil;
import javafx.scene.chart.ValueAxis;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;


/**
 * ClassName: TrafficSourceWindow
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/17 9:49
 * @Version 1.2
 */
public class TrafficSourceWindow {

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

        //按照mid分组
        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //mid 去重
        SingleOutputStreamOperator<TrafficSourceBean> trafficSourceDS = keyedByMidDS.map(new RichMapFunction<JSONObject, TrafficSourceBean>() {

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

                String curDt = DateFormatUtil.toDate(value.getLong("ts"));


                if (lastDt == null || !lastDt.equals(curDt)) {
                    uvCt = 1L;
                    valueState.update(curDt);
                }

                JSONObject common = value.getJSONObject("common");
                JSONObject page = value.getJSONObject("page");
                return TrafficSourceBean.builder()
                        .sc(common.getString("sc"))
                        .sid(common.getString("sid"))
                        .uvCt(uvCt)
                        .pageId(page.getString("page_id"))
                        .durSum(page.getLong("during_time"))
                        .ts(value.getLong("ts"))
                        .build();


            }
        });

       //按照sid分组
        KeyedStream<TrafficSourceBean, String> keyedBySidDS = trafficSourceDS.keyBy(TrafficSourceBean::getSid);

        //去重sid
        SingleOutputStreamOperator<TrafficSourceBean> trafficSourceSidDS = keyedBySidDS.map(new RichMapFunction<TrafficSourceBean, TrafficSourceBean>() {

            private ValueState<String> valueState;
            private MapState<String, Long> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {


                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("sid-state", String.class);

                MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("pageId-state", String.class, Long.class);

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();

                stateDescriptor.enableTimeToLive(ttlConfig);
                mapStateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);


            }

            @Override
            public TrafficSourceBean map(TrafficSourceBean value) throws Exception {

                String state = valueState.value();

                if (state == null) {
                    value.setSvCt(1L);
                    valueState.update("1");


                    String pageId = value.getPageId();
                    if (mapState != null && mapState.contains(pageId)) {
                        Long lastCt = mapState.get(pageId);
                        lastCt = lastCt + 1;
                        mapState.put(pageId, lastCt);
                    } else {
                        mapState.put(pageId, 1L);
                    }


                    //只有一个页面
                    long pageOne = 0L;
                    long pageSum = 0L;

                    for (Long aLong : mapState.values()) {
                        if (aLong == 1L) {
                            pageOne++;
                        }
                        pageSum += aLong;

                    }
                    value.setPageCt(pageSum);
                    value.setPageOneCt(pageOne);





                }
                return value;
            }
        });


        SingleOutputStreamOperator<TrafficSourceBean> reduceDS = trafficSourceSidDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficSourceBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficSourceBean>() {
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

                        value1.setPageCt(value1.getPageCt() + value2.getPageCt());

                        value1.setPageOneCt(value1.getPageOneCt() + value2.getPageOneCt());

                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());

                        return value1;
                    }
                }, new MyApplyUtil.MyWindowUtil<TrafficSourceBean, String>());

        reduceDS.print("reduceDS>>>>");


        env.execute();
    }

}
