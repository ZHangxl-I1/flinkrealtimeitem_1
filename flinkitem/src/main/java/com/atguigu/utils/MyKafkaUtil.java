package com.atguigu.utils;

import akka.stream.impl.FanIn;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * ClassName: MyKafkaUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/15 9:28
 * @Version 1.2
 */
public class MyKafkaUtil {


    private static  String BOOTSTRAP_SERVER = "hadoop104:9092";

    public static KafkaSource<String> getKafkaSource(String topic,String groupId) {


        return KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        //判断数据是否为空
                        if (message!=null){
                            return new String(message);
                        }else {

                            return null;
                        }
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();


    }

    //写入kafka主题
    public static <T>FlinkKafkaProducer<T>  getFlinkKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        return new FlinkKafkaProducer<T>("dwd_default_topic", kafkaSerializationSchema,properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);


    }

}
