package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.EDUConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {

    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //通过反射的方式获取T对象所有的字段
                        Class<?> aClass = t.getClass();
                        Field[] fields = aClass.getDeclaredFields();

                        //遍历fields,取出对应的值给占位符赋值
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            Field field = fields[i];
                            field.setAccessible(true);

                            //尝试获取注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }

                            Object value = field.get(t);

                            //给占位符赋值
                            preparedStatement.setObject(i + 1 - offset, value);
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(EDUConfig.CLICKHOUSE_DRIVER)
                        .withUrl(EDUConfig.CLICKHOUSE_URL)
                        .build());
    }

}
