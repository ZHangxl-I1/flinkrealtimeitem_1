package com.atguigu.app.func;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * ClassName: SplitFunction
 * Package: com.atguigu.app.func
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/18 14:19
 * @Version 1.2
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyword) {

        try {
            for (String s : KeywordUtil.splitKeyword(keyword)) {
                collect(Row.of(s));

            }
        } catch (Exception e) {
            collect(Row.of(keyword));
        }


    }


}


