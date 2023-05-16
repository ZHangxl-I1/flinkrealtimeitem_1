package com.atguigu.utils;

import com.atguigu.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: JdbcUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/15 10:02
 * @Version 1.2
 */
public class JdbcUtil {

    //将数据存放在List<任意类型>里

    public static <T> List<T> querList(Connection connection, String sql, Class<T> clz, Boolean underSourceToCamel) throws Exception {


        ArrayList<T> list = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //查询操作
        ResultSet resultSet = preparedStatement.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {

            T t = clz.newInstance();

            for (int i = 1; i < columnCount+1; i++) {

                String columnName = metaData.getColumnName(i);

                Object value = resultSet.getObject(columnName);


                //将数据库中的"-"字段替换成java的小驼峰
                if (underSourceToCamel) {

                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());

                }


                BeanUtils.setProperty(t, columnName, value);


            }

            list.add(t);

        }

        resultSet.close();
        preparedStatement.close();
        return list;

    }

    public static void main(String[] args) throws Exception {


        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/edu_config?" +
                "user=root&password=000000&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");

        String sql = "select * from table_process";
        List<TableProcess> tableProcesses = querList(connection, sql, TableProcess.class, true);

        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }

        connection.close();

    }


}
