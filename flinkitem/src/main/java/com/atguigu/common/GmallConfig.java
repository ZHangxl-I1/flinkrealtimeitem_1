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
public class GmallConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL221109REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/gmall_flink";


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



    //dwd
    //交易域加购事务事实表
    public static final String DWD_TRADE_CART_INFO = "dwd_trade_cart_info";
    //交易域下单事务事实表
    public static final String DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    //交易域取消订单事务事实表
    public static final String DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";
    //交易域支付成功事务事实表
    public static final String DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";
    //交易域退单事务事实表
    public static final String DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    //交易域退款成功事务事实表
    public static final String DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";



    //dwd层其他六张事实表主题
    //工具域优惠券领取事务事实表
    public static final String DWD_TOOL_COUPON_GET = "dwd_tool_coupon_get";
    //工具域优惠券使用事务事实表
    public static final String DWD_TOOL_COUPON_USE = "dwd_tool_coupon_use";
    //互动域收藏商品事务事实表
    public static final String DWD_INTERACTION_FAVOR_ADD = "dwd_interaction_favor_add";
    //互动域评价事务事实表
    public static final String DWD_INTERACTION_COMMENT = "dwd_interaction_comment";
    //用户域用户注册事务事实表
    public static final String DWD_USER_REGISTER = "dwd_user_register";




}
