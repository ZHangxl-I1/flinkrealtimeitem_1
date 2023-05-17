package com.atguigu.common;

/**
 * ClassName: GmallConfig
 * Package: com.atguigu.common
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/4/26 15:41
 * @Version 1.2
 */
public class EDUConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "EDU221109REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop104,hadoop105,hadoop106:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/edu_flink";


    //日志数据各kafka主题
    //页面
    public static final String PAGE_TOPIC = "dwd_traffic_page_log";
    //启动
    public static final String START_TOPIC = "dwd_traffic_start_log";
    //曝光
    public static final String DISPLAY_TOPIC = "dwd_traffic_display_log";
    //动作
    public static final String ACTION_TOPIC = "dwd_traffic_action_log";
    //错误
    public static final String ERROR_TOPIC = "dwd_traffic_error_log";
    //播放
    public static final String APPVIDEO_TOPIC = "dwd_traffic_appVideo_log";



    //dwd
    //交易域下单事务事实表
    public static final String DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    //交易域支付成功事务事实表
    public static final String DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";


    //课程域完成测验事实表
    public static final String DWD_TEST_EXAM = "dwd_test_exam";



    //dwd层其他六张事实表主题
    //收藏
    public static final String DWD_INTERACTION_FAVOR_ADD = "dwd_interaction_favor_add";
    //课程评价
    public static final String DWD_REVIEW_ADD = "dwd_review_add";
    //评价
    public static final String dwd_interaction_comment_add = "dwd_interaction_comment_add";
    //用户注册
    public static final String DWD_USER_REGISTER = "dwd_user_register";
    //vip
    public static final String DWD_VIP_CHANGE_DETAIL_ADD = "dwd_vip_change_detail_add";




}
