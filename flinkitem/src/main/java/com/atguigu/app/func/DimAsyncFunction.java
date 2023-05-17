package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * ClassName: DimAsyncFunction
 * Package: com.atguigu.app.func
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/5/17 15:04
 * @Version 1.0
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private ThreadPoolExecutor threadPoolExecutor;
    private DruidDataSource druidDataSource;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
        druidDataSource = DruidDSUtil.getDruidDataSource();
    }

    @Override
    public void close() throws Exception {
        druidDataSource.close();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                DruidPooledConnection connection = druidDataSource.getConnection();
                //读取维度表数据
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getKey(input));
                connection.close();
                //补充信息
                if (dimInfo != null) {
                    join(input, dimInfo);
                }
                //将补充信息后的数据再次写回流中
                resultFuture.complete(Collections.singleton(input));
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
