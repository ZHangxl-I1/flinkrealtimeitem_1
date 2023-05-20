package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TrafficSourceBean;
import com.atguigu.common.EDUConfig;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;
import scala.Int;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: TrafficSourceWindow02
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/17 19:38
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
                        .page(value.getJSONObject("page"))
                        .pageId(value.getJSONObject("page").getString("page_id"))
                        .sid(value.getJSONObject("common").getString("sid"))
                        .sc(value.getJSONObject("common").getString("sc"))
                        .uvCt(uvCt)
                        .ts(value.getLong("ts"))
                        .durSum(value.getJSONObject("page").getLong("during_time"))
                        .build();
            }
        });
//        SingleOutputStreamOperator<Integer> sidcount = keyedByMidDS.filter(new FilterFunction<JSONObject>() {
//                    @Override
//                    public boolean filter(JSONObject value) throws Exception {
//                        JSONObject page = value.getJSONObject("page");
//                        return page != null && page.size() == 1;
//                    }
//                }).map(json -> json.getJSONObject("common").getString("sid"))
//                .keyBy(sid -> sid)
//                .process(new ProcessFunction<String, Integer>() {
//                    private transient ValueState<Integer> sessionCountState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("sessionCount", Integer.class);
//                        sessionCountState = getRuntimeContext().getState(stateDescriptor);
//                    }
//
//                    @Override
//                    public void processElement(String sid, Context context, Collector<Integer> collector) throws Exception {
//                        Integer count = sessionCountState.value();
//                        if (count == null) {
//                            count = 1;
//                        } else {
//                            count++;
//                        }
//                        sessionCountState.update(count);
//                        collector.collect(count);
//                    }
//                });


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
                    if (value.getPage().getString("item_type") == null && value.getPage().getString("last_page_id") == null){
                        value.setPageOneCt(1L);
                    }

                }
                value.setPageCt(1L);

                return value;
            }


        });

//        SingleOutputStreamOperator<Tuple2<TrafficSourceBean, Long>> tuple2SingleOutputStreamOperator = trafficSourceSidDS.flatMap(new FlatMapFunction<TrafficSourceBean, Tuple2<TrafficSourceBean, Long>>() {
//            @Override
//            public void flatMap(TrafficSourceBean value, Collector<Tuple2<TrafficSourceBean, Long>> out) throws Exception {
//
//                out.collect(Tuple2.of(value, 1L));
//
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<TrafficSourceBean, Long>> sumDS = tuple2SingleOutputStreamOperator.keyBy(tuple -> tuple.f0)
//                .sum(1);
//
//
//        SingleOutputStreamOperator<TrafficSourceBean> trafficSourceBeanDS =sumDS.map(new RichMapFunction<Tuple2<TrafficSourceBean, Long>, TrafficSourceBean>() {
//            @Override
//            public TrafficSourceBean map(Tuple2<TrafficSourceBean, Long> value) throws Exception {
//                long pageOne=0L;
//               if (value.f1==1L){
//                   pageOne=1L;
//               }
//
//                return TrafficSourceBean.builder()
//                        .sc(value.f0.getSc())
//                        .uvCt(value.f0.getUvCt())
//                        .svCt(value.f0.getSvCt())
//                        .pageCt(value.f0.getPageCt())
//                        .pageOneCt(pageOne)
//                        .ts(value.f0.getTs())
//                        .build();
//            }
//        });


//        //按sid，和pageid分组
//        KeyedStream<TrafficSourceBean, Tuple2<String, String>> trafficSourceBeanTuple2KeyedStream = trafficSourceSidDS.keyBy(new KeySelector<TrafficSourceBean, Tuple2<String, String>>() {
//            @Override
//            public Tuple2<String, String> getKey(TrafficSourceBean value) throws Exception {
//                return new Tuple2<String, String>(value.getSid(), value.getPageId());
//            }
//        });
//
//
//
//
//        SingleOutputStreamOperator<TrafficSourceBean> trafficSourcePageIdDS = trafficSourceBeanTuple2KeyedStream.map(new RichMapFunction<TrafficSourceBean, TrafficSourceBean>() {
//
//            //            private ValueState<String> valueState;
//            private MapState<String, Long> mapState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//
//                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.hours(1))
//                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
//                        .build();
//
//
////                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("sid-page-state", String.class);
////                stateDescriptor.enableTimeToLive(ttlConfig);
////                valueState = getRuntimeContext().getState(stateDescriptor);
//
//
//                MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("pageId-state", String.class, Long.class);
//                mapStateDescriptor.enableTimeToLive(ttlConfig);
//                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
//            }
//
//            @Override
//            public TrafficSourceBean map(TrafficSourceBean value) throws Exception {
//                //              String state = valueState.value();
//                String pageId = value.getPageId();
//                String sid = value.getSid();
//                String key = pageId + "-" + sid;
//
////                value.setPageOne(1L);
////                valueState.update(key);
////                mapState.put(key,1L);
//
//                if (mapState.contains(key)) {
//                    Long lastCt = mapState.get(key);
//                    lastCt = lastCt + 1;
//                    mapState.put(key, lastCt);
//                } else {
//                    mapState.put(key, 1L);
//                }
//
////                long pageOne = 0L;
//
//                for (Long aLong : mapState.values()) {
//                    if (aLong == 1L) {
//                        value.setPageOneCt(1L);
//                    }
//                    mapState.remove(key);
//
//                }
////                System.out.println("pageOne>>>"+pageOne);
//
//
//                return value;
//
//            }
//
//
//        });

//        trafficSourcePageIdDS.print("trafficSourcePageIdDS>>>");

//        SingleOutputStreamOperator<TrafficSourceBean> processed = trafficSourceSidDS.filter(new FilterFunction<TrafficSourceBean>() {
//                    @Override
//                    public boolean filter(TrafficSourceBean value) throws Exception {
//                        return value.getPage().getString("item_type") == null && value.getPage().getString("last_page_id") == null;
//                    }
//                })
//                .keyBy(TrafficSourceBean::getSid)
//                .process(new ProcessFunction<TrafficSourceBean, TrafficSourceBean>() {
//
//                    private transient ValueState<Integer> sessionCountState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//
//                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("sessionCount", Integer.class);
//                        sessionCountState = getRuntimeContext().getState(stateDescriptor);
//                    }
//
//                    @Override
//                    public void processElement(TrafficSourceBean value, ProcessFunction<TrafficSourceBean, TrafficSourceBean>.Context ctx, Collector<TrafficSourceBean> out) throws Exception {
//
//                        Integer count = sessionCountState.value();
//                        if (count == null) {
//                            count = 1;
//                        } else {
//                            count++;
//                        }
//                        sessionCountState.update(count);
//                        System.out.println("count>>>" + count);
//
//                        out.collect(TrafficSourceBean.builder()
//                                .uvCt(value.getUvCt())
//                                .svCt(value.getSvCt())
//                                .sc(value.getSc())
//                                .pageCt(value.getPageCt())
//                                .durSum(value.getDurSum())
//                                .pageOneCt(Long.valueOf(count))
//                                .build());
//
//                    }
//                });


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

//                        value1.setPageOne(value1.getPageOne() + value2.getPageOne());

                        value1.setPageOneCt(value1.getPageOneCt() + value2.getPageOneCt());

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


        //关联来源表dim_base_source
        SingleOutputStreamOperator<TrafficSourceBean> resultDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<TrafficSourceBean>("DIM_BASE_SOURCE") {
            @Override
            public String getKey(TrafficSourceBean input) throws Exception {
                return input.getSc();
            }

            @Override
            public void join(TrafficSourceBean input, JSONObject dimInfo) throws Exception {

                input.setSourceName(dimInfo.getString("SOURCE_SITE"));

            }
        }, 60, TimeUnit.SECONDS);


        resultDS.print("resultDS>>>>");

        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_source_window values(?,?,?,?,?,?,?,?,?)"));

        env.execute();

    }
}
