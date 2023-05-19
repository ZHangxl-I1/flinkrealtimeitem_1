package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.VideoPlayBean;
import com.atguigu.common.EDUConfig;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyApplyUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: VideoPlayWindow
 * Package: com.atguigu.app.dws
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/18 15:02
 * @Version 1.2
 */
public class VideoPlayWindow {
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

        /**
         * {
         *   "appVideo": {						--视频信息
         *     "play_sec": 19,					--播放时长
         * "position_sec":390,				--播放进度
         *     "video_id": "3904"				--视频id
         *   },
         *   "common": {
         *     "ar": "4",
         *     "ba": "Sumsung",
         *     "ch": "oppo",
         *     "is_new": "0",
         *     "md": "Sumsung Galaxy S20",
         *     "mid": "mid_253",
         *     "os": "Android 11.0",
         *     "sc": "1",
         *     "sid": "47157c4a-4790-4b9a-a859-f0d36cd62a10",
         *     "uid": "329",
         *     "vc": "v2.1.134"
         *   },
         *   "err":{
         *     "error_code":3485,
         *     "msg":"java.net.SocketTimeoutException"
         *   },
         *   "ts": 1645526307119
         * }
         */

        //从kafka读取 dwd_traffic_appVideo_log数据
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaSource(EDUConfig.APPVIDEO_TOPIC, "video_play_window"), WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                JSONObject jsonObject = JSON.parseObject(element);
                return jsonObject.getLong("ts");
            }
        }), "kafka-source");


        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                if (value != null) {
                    out.collect(JSONObject.parseObject(value));
                }

            }
        });

        SingleOutputStreamOperator<VideoPlayBean> videoPlayDS = jsonObjDS.map(new MapFunction<JSONObject, VideoPlayBean>() {
            @Override
            public VideoPlayBean map(JSONObject value) throws Exception {

                return VideoPlayBean.builder()
                        .videoPlayCt(1L)
                        .palySec(value.getJSONObject("appVideo").getLong("play_sec"))
                        .videoId(value.getJSONObject("appVideo").getString("video_id"))
                        .userId(value.getJSONObject("common").getString("uid"))
                        .ts(value.getLong("ts"))
                        .build();


            }
        });

        //按照uid分组去重
        KeyedStream<VideoPlayBean, String> videoPlayBeanStringKeyedStream = videoPlayDS.keyBy(VideoPlayBean::getUserId);


        SingleOutputStreamOperator<VideoPlayBean> videoPlayUidDS = videoPlayBeanStringKeyedStream.map(new RichMapFunction<VideoPlayBean, VideoPlayBean>() {


            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.hours(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("uid-state", String.class);

                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public VideoPlayBean map(VideoPlayBean value) throws Exception {
                String lastDt = valueState.value();

                String curDt = DateFormatUtil.toDate(value.getTs());

                if (lastDt == null || !lastDt.equals(curDt)) {
                    value.setUserCt(1L);
                    valueState.update(curDt);
                }

                return value;

            }
        });



        //关联dim_video_info
        SingleOutputStreamOperator<VideoPlayBean> videoPlayChapterDS = AsyncDataStream.unorderedWait(videoPlayUidDS, new DimAsyncFunction<VideoPlayBean>("DIM_VIDEO_INFO") {
            @Override
            public String getKey(VideoPlayBean input) throws Exception {
                return input.getVideoId();
            }

            @Override
            public void join(VideoPlayBean input, JSONObject dimInfo) throws Exception {
                input.setChapterId(dimInfo.getString("CHAPTER_ID"));

            }
        }, 60, TimeUnit.SECONDS);



        //分组开窗聚合
        SingleOutputStreamOperator<VideoPlayBean> reduceDS = videoPlayChapterDS.keyBy(VideoPlayBean::getChapterId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<VideoPlayBean>() {
                    @Override
                    public VideoPlayBean reduce(VideoPlayBean value1, VideoPlayBean value2) throws Exception {


                        value1.setVideoPlayCt(value1.getVideoPlayCt() + value2.getVideoPlayCt());

                        value1.setPalySec(value1.getPalySec() + value2.getPalySec());

                        value1.setUserCt(value1.getUserCt() + value2.getUserCt());

                        return value1;


                    }
                }, new MyApplyUtil.MyWindowUtil<VideoPlayBean, String>());


        //关联dim_chapter_info
        SingleOutputStreamOperator<VideoPlayBean> videoPlayChapterNameDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<VideoPlayBean>("DIM_CHAPTER_INFO") {
            @Override
            public String getKey(VideoPlayBean input) throws Exception {
                return input.getChapterId();
            }

            @Override
            public void join(VideoPlayBean input, JSONObject dimInfo) throws Exception {
                input.setChapterName(dimInfo.getString("CHAPTER_NAME"));

            }
        }, 60, TimeUnit.SECONDS);


        videoPlayChapterNameDS.print("videoPlayChapterNameDS>>>>");


        videoPlayChapterNameDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_video_info_window values(?,?,?,?,?,?,?)"));


        env.execute();
















//        //按照视频id分组
//        KeyedStream<JSONObject, String> keyedByVideoIdDS = jsonObjDS.keyBy(json -> json.getJSONObject("appVideo").getString("video_id"));


//
//        SingleOutputStreamOperator<VideoPlayBean> videoPlayDS = keyedByVideoIdDS.map(new RichMapFunction<JSONObject, VideoPlayBean>() {
//
//            private ValueState<String> valueState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//
//                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
//                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
//                        .build();
//
//                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("video-state", String.class);
//
//                stateDescriptor.enableTimeToLive(ttlConfig);
//                valueState = getRuntimeContext().getState(stateDescriptor);
//
//            }
//
//
//            @Override
//            public VideoPlayBean map(JSONObject value) throws Exception {
//
//                String lastDtId = valueState.value();
//                String curDt = DateFormatUtil.toDate(value.getLong("ts"));
//
//                //将当天日期和video_id作为标识，同一天的时间和id会相同，不是同一天的时间和id拼起来不相同
//                String curDtId = curDt + "-" + value.getJSONObject("appVideo").getString("video_id");
//                Long playCt = 0L;
//                if (lastDtId == null || !lastDtId.equals(curDtId)) {
//                    playCt = 1L;
//                    valueState.update(curDtId);
//                }
//
//                return VideoPlayBean.builder()
//                        .userId(value.getJSONObject("common").getString("uid"))
//                        .videoPlayCt(playCt)
//                        .palySec(value.getJSONObject("appVideo").getLong("play_sec"))
//                        .build();
//
//            }
//        });


    }
}
