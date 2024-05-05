package com.atguigu.gmall.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: KeywordUtil
 * Package: com.atguigu.gmall.realtime.dws.util
 * Description:
 *      分词工具类
 * @Author Wang Anning
 * @Create 2024/4/24 11:11
 * @Version 1.0
 */
public class KeywordUtil {
    // 分词方法
    public static List<String> analyze(String text){
        ArrayList<String> wordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        // 第2个参数：是否智能分词
        // 非智能，输出：[小米, 手机, 手, 机, 京东, 自营, 5g, 5, g, 联通, 通电, 电信, 移动]
        // 智能，输出：[小米, 手机, 京东, 自营, 5g, 联通, 电信, 移动]
        IKSegmenter ik = new IKSegmenter(reader,true);
        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next()) != null){
                String keyword = lexeme.getLexemeText();
                wordList.add(keyword);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return wordList;
    }
    // 测试
    public static void main(String[] args) {
        System.out.println(analyze("小米手机京东自营5G联通电信移动"));
    }
}
