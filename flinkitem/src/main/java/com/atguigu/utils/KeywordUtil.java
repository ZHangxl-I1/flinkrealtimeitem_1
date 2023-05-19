package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: KeywordUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/18 14:14
 * @Version 1.2
 */
public class KeywordUtil {


    public static List<String> splitKeyword(String keyword) throws Exception {


        ArrayList<String> list = new ArrayList<>();

        //创建Ik分词器对象
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyword), false);

        Lexeme next = ikSegmenter.next();
        while (next!=null){

            String word = next.getLexemeText();
            list.add(word);
           next= ikSegmenter.next();
        }


        return list;

    }


}
