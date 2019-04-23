package Flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author wangjx
 * @date 2019/4/10 14:00
 * 使用java api来开发flink的批处理应用程序
 */
public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {
        String input = "file:///Users/wangjx/data/log.txt";
        //step1:获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //step2:read data
        DataSource<String> text = env.readTextFile(input);
        //step3:transform
        //flatmap 传进来是一个单词 传出是单词与数量 word=>(word,1)
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            //value就是一行一行的字符串
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //分隔完形成一个string的数组
                String[] tokens = value.split(" ");
                for (String token: tokens) {
                    if(token.length()>0){
                        //用collector发送出去
                        collector.collect(new Tuple2<String, Integer>(token,1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();

    }
}
