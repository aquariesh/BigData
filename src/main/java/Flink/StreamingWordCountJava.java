package Flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author wangjx
 * @date 2019/4/10 14:56
 * 使用java api来开发flink的实时处理应用程序
 * word count 的数据源自socket
 */
public class StreamingWordCountJava {
    public static void main(String[] args) throws Exception {
        //step1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //step2 读取socket数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
        //step3 transform
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
        }).keyBy(0).sum(1).print().setParallelism(1);

        env.execute("StreamingWordCountJava");
    }
}
