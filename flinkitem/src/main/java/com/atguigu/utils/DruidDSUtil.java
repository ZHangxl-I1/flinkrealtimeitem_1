package com.atguigu.utils;

/**
 * ClassName: DruidDSUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/4/26 15:40
 * @Version 1.2
 */

//连接池

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.atguigu.common.GmallConfig;

import java.sql.SQLException;

//连接池
public class DruidDSUtil {
    private static DruidDataSource druidDataSource;

    static {
        // 创建连接池
        druidDataSource = new DruidDataSource();
        // 设置驱动全类名
        druidDataSource.setDriverClassName(GmallConfig.PHOENIX_DRIVER);
        // 设置连接 url
        druidDataSource.setUrl(GmallConfig.PHOENIX_SERVER);
        // 设置初始化连接池时池中连接的数量
        druidDataSource.setInitialSize(5);
        // 设置同时活跃的最大连接数
        druidDataSource.setMaxActive(20);
        // 设置空闲时的最小连接数，必须介于 0 和最大连接数之间，默认为 0
        druidDataSource.setMinIdle(1);
        // 设置没有空余连接时的等待时间，超时抛出异常，-1 表示一直等待
        druidDataSource.setMaxWait(-1);
        // 验证连接是否可用使用的 SQL 语句
        druidDataSource.setValidationQuery("select 1");
        // 指明连接是否被空闲连接回收器（如果有）进行检验，如果检测失败，则连接将被从池中去除
        // 注意，默认值为 true，如果没有设置 validationQuery，则报错
        // testWhileIdle is true, validationQuery not set
        druidDataSource.setTestWhileIdle(true);
        // 借出连接时，是否测试，设置为 true，不测试可能连接无法使用
        druidDataSource.setTestOnBorrow(true);
        // 归还连接时，是否测试
        druidDataSource.setTestOnReturn(false);
        // 设置空闲连接回收器每隔 30s 运行一次
        druidDataSource.setTimeBetweenEvictionRunsMillis(30 * 1000L);
        // 设置池中连接空闲 30min 被回收，默认值即为 30 min
        druidDataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000L);
    }

    //可直接获取连接池信息
    public static DruidPooledConnection getPhoenixConn() throws SQLException {
        return druidDataSource.getConnection();
    }


    //需要创建连接才可以获取连接池
    public static DruidDataSource getDruidDataSource(){
        return druidDataSource;
    }
}
